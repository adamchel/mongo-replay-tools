from paramiko import SSHClient
from argparse import ArgumentParser
from subprocess import Popen # poll, send_signal, communicate

def start_dump(msaw_dir):
	pass

def connect_to_primary(host):
	client = SSHClient()
	client.load_system_host_keys()
	client.connect(host, username="user")

def start_recording(msaw_dir):
	pass

#	stdin, stdout, stderr = client.exec_command('program')
#	print "stderr: ", stderr.readlines()
#	print "pwd: ", stdout.readlines()

	pass

def clean_recording(msaw_dir):
	pass

def get_args():
	parser = ArgumentParser(prog="mongocapture")

	parser.add_argument('PRIMARY_HOST', help='The primary node on which to record network traffic.')
	parser.add_argument('PRIMARY_PORT', help='The port of mongod on the primary node on which to record network traffic.')
	parser.add_argument('SECONDARY_HOST', help='The secondary node from which to create a state backup.')
	parser.add_argument('SECONDARY_PORT', help='The port of the secondary node.')

	return parser.parse_args()

if __name__ == "__main__":
	print get_args()

