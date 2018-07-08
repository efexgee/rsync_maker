#!/usr/bin/env python3

import argparse
import configparser
import os

RSYNC_CMD = "sudo -u falko -g IHME-SA rsync"
CONFIG_FILE = "/homes/falko/.rsync_nanny.ini"
ROOT_PATH = "/qumulo"

### Need to error on missing .ini file

def main():
	nodes = {}

	args = get_args()
	#print(args)

	with open(args.paths_file) as f:
                #paths = f.readlines()
                paths = f.read().splitlines()

	#print(paths)

	config = get_config(CONFIG_FILE)
	#print(config.sections())
	#print(config["rsync"]["opts"])

	nodes[args.src_cluster] = {"node_{:02}".format(i):0 for i in range(1, int(config[args.src_cluster]['nodes']) + 1)}

	#print(nodes)

	while paths:
		path = paths.pop(0)	# pop from the front/head
		#print("path=", path)
		src_directory = assemble_dir(args.src_cluster, path, nodes)
		dst_directory = os.path.join(args.dst_path, "")
		#dst_directory = os.path.normpath(args.dst_path + "/")

		launch_rsync(args.test, config["rsync"]["opts"], src_directory, dst_directory, path)
	

def get_args():
	parser = argparse.ArgumentParser(description="This tells you what this thing does")
	parser.add_argument("src_cluster", metavar="<source cluster>", help="source cluster")
	parser.add_argument("dst_path", metavar="<dst_path>", help="dest path")
	parser.add_argument("paths_file", metavar="<paths file>", help="file containing paths")
	parser.add_argument("--test", dest="test", action = "store_true", help="don't do anything")

	args = parser.parse_args()

	if args.dst_path is None:
		args.dst_path = args.src_path

	return args

def get_config(config_file):
	config = configparser.ConfigParser()
	config.read(config_file)
	return config

def get_node(nodes, cluster):
	nodes = nodes[cluster]
	node = sorted(sorted(nodes.keys()), key = lambda k:nodes[k])[0]

	# incrementing the node use count here though that's dirty
	nodes[node] += 1

	return node

def assemble_dir(cluster, path, nodes):
	node = get_node(nodes, cluster)
	directory = os.path.join(ROOT_PATH, cluster, node, path)
	return directory

def launch_rsync(dry_run, rsync_opts, src_dir, dst_dir, path):
	# now I am going to developer hell
	file_base = path.replace('/','.')
	log_file = file_base + ".log"
	out_file = file_base + ".out"

	if dry_run:
		dry_run = "--dry-run"
	else:
		dry_run = ""

	command_line = [RSYNC_CMD, dry_run, rsync_opts, "--log-file", log_file, src_dir, dst_dir, ">", out_file, "2>&1", "&"]

	print(" ".join(command_line))

if __name__ == "__main__":
	main()
