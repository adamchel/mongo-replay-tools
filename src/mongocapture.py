from paramiko import SSHClient
from argparse import ArgumentParser
from subprocess import Popen # poll, send_signal, communicate
import signal

def get_dump(mstate_dir, mtools_dir, host, port):
	dumper = Popen(['mongodump', '--oplog', '--host', str(host) + ':' + str(port), '--out=' + str(mstate_dir)], cwd=mtools_dir)

def ssh_to_primary(host):
	client = SSHClient()
	client.load_system_host_keys()
	client.connect(host, username="adam")

def start_recording(ssh_client, msaw_dir, primary_port, primary_host):

	# TODO connect to a primary on a different host, output the pcap, and scp it over to the localhost
	stdin, stdout, stderr = ssh_client.exec_command('sudo tcpdump -i lo0 -w workload.pcap dst port' + str(primary_port))

	#copy = Popen("scp root@" + str(primary_host) + ":workload.pcap /some/local/directory")

	print "stderr: ", stderr.readlines()
	print "pwd: ", stdout.readlines()

#def clean_recording(msaw_dir):
#	pass

def get_args():
	parser = ArgumentParser(prog="mongocapture")

	parser.add_argument('PRIMARY_HOST', help='The primary node on which to record network traffic.')
	parser.add_argument('PRIMARY_PORT', help='The port of mongod on the primary node.')
	parser.add_argument('SECONDARY_HOST', help='The secondary node from which to create a state backup.')
	parser.add_argument('SECONDARY_PORT', help='The port of the secondary node.')
	parser.add_argument('--mongo-tools-dir', help='The directory where mongodump can be found.', default=None)

	return parser.parse_args()

if __name__ == "__main__":
	args = get_args()

	dump_proc = get_dump()
	dump_proc.wait()

	record_proc = start_recording()



