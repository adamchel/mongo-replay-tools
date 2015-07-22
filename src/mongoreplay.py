import ast
import json
import os
import pymongo
import shutil
import subprocess
import sys
import time
import yaml

from argparse import ArgumentParser
from pprint import pprint

def parse_sniff_for_crud(filename):
	"""
	Parses the output from the mongosniff operation for raw CRUD stats.
	"""
	c = 0
	r = 0
	u = 0
	d = 0

	f = open(filename, 'r')

	for line in f:
		if "query: { insert:" in line:
			c = c + 1
		elif "query: { delete:" in line:
			d = d + 1
		elif "query: { update:" in line:
			u = u + 1
		elif "query: {" in line:
			r = r + 1

	f.close()

	total_crud = float(c + r + u + d)
	return total_crud, c, r, u, d

def get_crud_stats(filename):
	"""
	Calculates essential CRUD stats.
	"""
	print "Getting crud stats..."
	crud_stats = parse_sniff_for_crud(filename)

	total_crud = crud_stats[0]
	c = crud_stats[1]
	r = crud_stats[2]
	u = crud_stats[3]
	d = crud_stats[4]

	total_read_percent = r / total_crud
	total_write_percent = (c + u + d) / total_crud

	return total_read_percent, total_write_percent

def read_server_config(server_config_file):
	with open(server_config_file, 'r') as stream:
		yaml_stream = yaml.load(stream)
		servers = yaml_stream['servers']
		db_path = yaml_stream['tmp_db_path']
	return servers, db_path

def replay(host, port, servers, state, workload, db_path):
	"""Replays on all servers and collect data"""
	print "Replaying workload on servers..."
	performance_metrics = {}
	for server_config in servers:
		print "Replaying workload on " + server_config[16:] + " config"
		metric = replay_and_aggregate(host, port, server_config, state, workload, db_path)
		performance_metrics[server_config] = metric
		time.sleep(1)
	return performance_metrics

def get_workload_stats(host, port, db_path, state, workload):
	"""Gets the specific statistics about the workload (avgObjSize, r/w ratio)"""
	workload_stats = {}
	md = create_mongod("", db_path)
	load_data(state)
	play_workload(host, port, workload)
	c = pymongo.MongoClient(host, int(port))
	print c.database_names()
	for db in c.database_names():
		if db != "local":
			print "Getting workload stats for " + db + "..."
			db_conn = c[db]
			db_stat = db_conn.command("dbstats")
			# Hack to get mongo dbstat output to not be unicode strings
			db_stat = ast.literal_eval(json.dumps(db_stat))
			workload_stats[str(db)] = db_stat
	kill_mongod(md, db_path)

	crud_stats = get_crud_stats("sniff.out")
	workload_stats["CRUD"] = {"% read": crud_stats[0], "% write": crud_stats[1]}

	return workload_stats

def replay_and_aggregate(host, port, server_config, state, workload, db_path):
	"""Replay on a single server and aggregate data from that server"""
	md = create_mongod(server_config, db_path)
	load_data(state)
	metric = play_workload(host, port, workload, quiet=True)
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
	config.append("--logpath=" + db_path + "/mongosniff.log")
	process = subprocess.Popen(config)
	return process

def kill_mongod(process, db_path):
	process.kill()
	# Clear the DB dir
	shutil.rmtree(db_path, ignore_errors=True)

def load_data(state):
	"""Load the state into the running mongod server"""
	subprocess.call(["mongorestore", "--maintainInsertionOrder", state])

def play_workload(host, port, workload, quiet=False):
	metric = {}
	if quiet:
		with open(os.devnull, 'w') as devnull:
			load_workload = subprocess.Popen([
				"mongo-tcp-replay/mongo-tcp-replay",
				"-mongodHost",
				host,
				"-mongodPort",
				port,
				"-pcapFile",
				workload
			], stdout=devnull)
	else:
		f = open("sniff.out", 'w+')
		load_workload = subprocess.Popen([
			"mongosniff",
			"--source",
			"FILE",
			workload,
			"--forward",
			host + ":" + port
		], stdout=f)
		f.close()
	# Get the resource usage of the workload
	ru = os.wait4(load_workload.pid, 0)[2]
	workload_time = ru.ru_utime + ru.ru_stime
	metric['workload_time'] = workload_time
	return metric

def change_port(source, dest, infile, outfile):
	from uuid import getnode as get_mac
	mac = hex(get_mac())
	subprocess.Popen([
		"tcprewrite",
		"--portmap=" + source + ":" + dest,
		"--infile=" + infile,
		"--outfile=" + outfile,
		"--dlt=enet",
		"--enet-dmac=" + mac,
		"--enet-smac=" + mac
	])

def get_args():
	parser = ArgumentParser(prog="mongoreplay")

	parser.add_argument('--mongod-host', default='localhost', help='address of mongod host')
	parser.add_argument('--mongod-port', default='27017', help='port of mongod at host')
	parser.add_argument('--server-config', default='server_config.yml', help='Location of the mongod config file.')
	parser.add_argument('--state-dump', default='state_dump', help='Location of the state dump directory.')
	parser.add_argument('--workload-file', default='workload.pcap', help='Location of the workload pcap file.')
	parser.add_argument('--original-host-port', default='30000', help='The original host port number.')

	return parser.parse_args()

if __name__ == "__main__":
	args = get_args()

	mongod_host = args.mongod_host
	mongod_port = args.mongod_port
	servers, db_path = read_server_config(args.server_config)
	state = args.state_dump
	workload = args.workload_file
	original_host_port = args.original_host_port

	outfile = "sniff.pcap"
	change_port(original_host_port, mongod_port, workload, outfile)
	workload_stats = get_workload_stats(mongod_host, mongod_port, db_path, state, outfile)
	performance_metrics = replay(mongod_host, mongod_port, servers, state, workload, db_path)

	print "Workload stats:"
	pprint(workload_stats)
	print "Performance metrics:"
	pprint(performance_metrics)
