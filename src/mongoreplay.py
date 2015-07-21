import ast
import json
import os
import pymongo
import shutil
import subprocess
import sys
import yaml

from argparse import ArgumentParser

def read_server_config(server_config_file):
	with open(server_config_file, 'r') as stream:
		yaml_stream = yaml.load(stream)
		servers = yaml_stream['servers']
		db_path = yaml_stream['tmp_db_path']
	return servers, db_path

def replay(servers, state, workload, db_path):
	"""Replays on all servers and collect data"""
	performance_metrics = {}
	for server_config in servers:
		metric = replay_and_aggregate(server_config, state, workload, db_path)
		performance_metrics[server_config] = metric
	return performance_metrics

def get_workload_stats(db_path, workload):
	"""Gets the specific statistics about the workload (avgObjSize, r/w ratio)"""
	workload_stats = {}
	md = create_mongod("", db_path)
	play_workload(workload)
	c = pymongo.MongoClient(host = "127.0.0.1", port = 27017)
	for db in c.database_names():
		if db != "local":
			db_conn = c[db]
			db_stat = db_conn.command("dbstats")
			# Hack to get mongo dbstat output to not be unicode strings
			db_stat = ast.literal_eval(json.dumps(db_stat))
			workload_stats[str(db)] = db_stat
	kill_mongod(md, db_path)
	return workload_stats

def replay_and_aggregate(server_config, state, workload, db_path):
	"""Replay on a single server and aggregate data from that server"""
	md = create_mongod(server_config, db_path)
	load_data(state)
	metric = play_workload(workload, quiet=True)
	kill_mongod(md, db_path)
	return metric

def create_mongod(server_config, db_path):
	"""Creates a mongod instance from a server config"""
	# Ensure the DB dir is nonexistant
	shutil.rmtree(db_path, ignore_errors=True)
	# Make the temporary DB dir
	os.makedirs(db_path)
	config = ["mongod"]
	config += ([] if server_config == "" else server_config.split(" "))
	config.append("--dbpath="+db_path)
	process = subprocess.Popen(config)
	return process

def kill_mongod(process, db_path):
	process.kill()
	# Clear the DB dir
	shutil.rmtree(db_path, ignore_errors=True)

def load_data(state):
	"""Load the state into the running mongod server"""
	subprocess.call(["mongorestore", "--maintainInsertionOrder", state])

def play_workload(workload, quiet=False):
	metric = {}
	if quiet:
		with open(os.devnull, 'w') as devnull:
			load_workload = subprocess.Popen([
				"mongosniff",
				"--source",
				"FILE",
				workload,
				"--forward",
				"127.0.0.1:27017"
			], stdout=devnull)
	else:
		load_workload = subprocess.Popen([
			"mongosniff",
			"--source",
			"FILE",
			workload,
			"--forward",
			"127.0.0.1:27017"
		])
	# Get the resource usage of the workload
	ru = os.wait4(load_workload.pid, 0)[2]
	workload_time = ru.ru_utime + ru.ru_stime
	metric['workload_time'] = workload_time
	return metric

def get_args():
	parser = ArgumentParser(prog="mongoreplay")

	parser.add_argument('--server_config', default='server_config.yml', help='Location of the mongod config file.')
	parser.add_argument('--state_dump', default='dump', help='Location of the state dump directory.')
	parser.add_argument('--workload_file', default='workload.pcap', help='Location of the workload pcap file.')

	return parser.parse_args()

if __name__ == "__main__":
	servers, db_path = read_server_config(get_args().server_config)
	state = get_args().state_dump
	workload = get_args().workload_file

	workload_stats = get_workload_stats(db_path, workload)
	performance_metrics = replay(servers, state, workload, db_path)
	print workload_stats
	print performance_metrics
