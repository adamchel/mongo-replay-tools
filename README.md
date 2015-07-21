# Mongo Replay Tools

Set up test mongod instances with given configurations and replay a target workload on them. The workload applied is in the form of pcap and is captured from a production workload with mongocapture.

## Usage

```shell

	git clone https://github.com/adamchel/mongo-replay-tools
	cd mongo-replay-tools

	# Capture the existing state of the DB
	./mongocapture

 	# Modify as desired
	cp server_config_example.yml server_config.yml

	# Replay the existing state and captured workload on each server in server_config
	./mongoreplay

```