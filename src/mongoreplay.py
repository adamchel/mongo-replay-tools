from argparse import ArgumentParser
import os
import shutil
import subprocess
import sys
import yaml

def read_server_config(server_config_file):
	with open(server_config_file, 'r') as stream:
		servers = yaml.load(stream)['servers']
	return servers

def replay(servers, state, workload):
	"""Replays on all servers and collect data"""
	for server_config in servers:
		replay_and_aggregate(server_config, state, workload)

def replay_and_aggregate(server_config, state, workload):
	"""Replay on a single server and aggregate data from that server"""
	md = create_mongod(server_config)
	load_data(state)
	metrics = play_workload(workload)
	md.kill()

def create_mongod(server_config):
	"""Creates a mongod instance from a server config"""
	test_db = "tmpdb/data"
	shutil.rmtree(test_db, ignore_errors=True) # Clear the DB dir
	os.makedirs(test_db) # Remake the DB dir
	process = subprocess.Popen(["mongod", server_config, "--dbpath="+test_db])
	return process

def load_data(state):
	"""Load the state into the running mongod server"""
	subprocess.call(["mongorestore", "--maintainInsertionOrder", state])

def play_workload(workload):
	subprocess.call(["mongosniff", "--source FILE" + workload, "--forward 127.0.0.1:27017"])

def get_args():
	parser = ArgumentParser(prog="mongoreplay")

	parser.add_argument('--server_config', default='server_config.yml', help='Location of the mongod config file.')
	parser.add_argument('--state_dump', default='dump', help='Location of the state dump directory.')
	parser.add_argument('--workload_file', default='workload.pcap', help='Location of the workload pcap file.')

	return parser.parse_args()

if __name__ == "__main__":
	servers = read_server_config(get_args().server_config)
	state = get_args().state_dump
	workload = get_args().workload_file

	replay(servers, state, workload)