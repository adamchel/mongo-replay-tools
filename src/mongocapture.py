from paramiko import SSHClient
from argparse import ArgumentParser
from subprocess import Popen, call # poll, send_signal, communicate
from getpass import getpass
import select

def get_dump(mstate_dir, mtools_dir, host, port):
	print("Attempting to save state of the secondary...")
	dumper = Popen([mtools_dir + '/mongodump', '--oplog', '--host', str(host) + ':' + str(port), '--out=' + str(mstate_dir)])
	dumper.wait()
	if(dumper.returncode != 0):
		print("Failed to run mongodump.")
		sys.exit(1)

def ssh_to_primary(username, host, port, password):
	client = SSHClient()
	client.load_system_host_keys()
	client.connect(host, username=username, password=password, port=port)
	return client

def print_from_paramiko_stream(stream):
	while(stream.recv_ready()):
		print(stream.recv(512))
	while(stream.recv_stderr_ready()):
		print(stream.recv_stderr(512))

def record_workload(ssh_client, workload_dir, primary_host, primary_port, primary_username, primary_password, loopback_device, ssh_port):
	chan = ssh_client.get_transport().open_session()
	chan.exec_command("sudo -S -p '' tcpdump -i " + loopback_device + " -w workload.pcap dst port " + str(primary_port))

	while(chan.send(primary_password + "\n") == 0):
		print "password failed to send..."
		pass

	print("Attempting to listen to network traffic on the primary...")
	interrupted = False
	while(not interrupted):
		try:
			streams = select.select([chan], [], [])
			for stream in streams[0]:
				print_from_paramiko_stream(stream)
		except KeyboardInterrupt:
			interrupted = True
			# Finish any incomplete output
			print_from_paramiko_stream(stream)

	print("\n")
	# Stop tcpdump on the primary host
	killchan = ssh_client.get_transport().open_session()
	killchan.exec_command("sudo -S -p '' killall tcpdump")
	while(killchan.send(primary_password + "\n") == 0):
		print "password failed to send..."
		pass
	killchan.recv_exit_status()

	# Print any resulting output
	chan.recv_exit_status()
	print_from_paramiko_stream(stream)

	# Copy the tcpdump to the local machine
	print("\nYou will be prompted again for your SSH password on the primary host, to retrieve the workload.")
	if(call(["scp", "-P", ssh_port, str(primary_username) + "@" + str(primary_host) + ":workload.pcap", "./"]) != 0):
		print("Failed to copy captured workload from primary.")
		sys.exit(1)

def get_args():
	parser = ArgumentParser(prog="mongocapture")

	parser.add_argument('PRIMARY_HOST', help='The primary node on which to record network traffic.')
	parser.add_argument('PRIMARY_PORT', help='The port of mongod on the primary node.')
	parser.add_argument('SECONDARY_HOST', help='The secondary node from which to create a state backup.')
	parser.add_argument('SECONDARY_PORT', help='The port of the secondary node.')
	parser.add_argument('--mdir', help='The directory where mongo tools (mongodump) can be found.', default="./")
	parser.add_argument('--ssh-port', help='The SSH port of the primary host, if not 22.', default=22)
	parser.add_argument('--net-device', help='The loopback device on the primary host.', default='lo0')

	return parser.parse_args()

if __name__ == "__main__":
	args = get_args()

	# Connect to the primary to prepare for TCP dump.
	print("Attempting to make an SSH connection to the primary host.")
	username = raw_input("Primary host username: ")
	password = getpass("Primary host password: ")
	primary_client = ssh_to_primary(username, args.PRIMARY_HOST, int(args.ssh_port), password)

	# Get a dump of the secondary and save it as the state.
	get_dump("state_dump", args.mdir, args.SECONDARY_HOST, args.SECONDARY_PORT)
	
	# Use TCP dump to record the network traffic going into the primary.
	record_workload(primary_client, ".", args.PRIMARY_HOST, args.PRIMARY_PORT, username, password, args.net_device, args.ssh_port)
