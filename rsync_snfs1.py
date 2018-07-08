#!/usr/bin/env python3

import argparse
import configparser
import os

RSYNC_CMD = "sudo -u falko -g IHME-SA rsync"
CONFIG_FILE = "/homes/falko/.rsync_nanny.ini"
ROOT_PATH = "/qumulo"
DEST_ROOT_PATH = "/2015_Archive.BackedUp"

### Need to error on missing .ini file

def main():
	nodes = {}

	args = get_args()
	#print(args)

	bucket_list = sorted(os.listdir(args.bucket_dir))

	#print(paths)

	config = get_config(CONFIG_FILE)
	#print(config.sections())
	#print(config["rsync"]["opts"])

	nodes[args.src_cluster] = {"node_{:02}".format(i):0 for i in range(1, int(config[args.src_cluster]['nodes']) + 1)}

	#print(nodes)

	
	while bucket_list:
		src_directory = assemble_dir(args.src_cluster, args.src_path, nodes)
		dst_directory = os.path.join(DEST_ROOT_PATH,args.dst_path)

		bucket_file = get_bucket(args.bucket_dir, bucket_list)
		launch_rsync(args.test, config["rsync"]["opts"], src_directory, dst_directory, bucket_file)


def get_args():
	parser = argparse.ArgumentParser(description="This tells you what this thing does")
	parser.add_argument("src_cluster", metavar="<source cluster>", help="source cluster")
	parser.add_argument("src_path", metavar="<src path>", help="source path on the Qumulo")
	parser.add_argument("dst_path", metavar="<dst_path>", help="destination path under $DEST_ROOT_PATH")
	parser.add_argument("bucket_dir", metavar="<bucket dir>", help="directory containing buckets")
	parser.add_argument("--test", dest="test", action = "store_true", help="don't do anything")

	args = parser.parse_args()

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

def get_bucket(bucket_dir, bucket_list):
	bucket_file = os.path.join(bucket_dir, bucket_list.pop(0))
	return bucket_file

def launch_rsync(dry_run, rsync_opts, src_dir, dst_dir, bucket_file):
	# now I am going to developer hell
	file_base = os.path.basename(bucket_file)
	log_file = file_base + ".log"
	out_file = file_base + ".out"

	if dry_run:
		dry_run = "--dry-run"
	else:
		dry_run = ""

	#command_line = [RSYNC_CMD, dry_run, rsync_opts, "--log-file", log_file, src_dir, dst_dir, ">", out_file, "2>&1", "&"]
	command_line = [RSYNC_CMD, dry_run, rsync_opts, "--files-from", bucket_file, "--log-file", log_file, src_dir, dst_dir + "/", ">", out_file, "2>&1", "&"]


	print(" ".join(command_line))

if __name__ == "__main__":
	main()
