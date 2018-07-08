#!/usr/bin/env python3

import argparse
import configparser
import os
from collections import defaultdict

RSYNC_CMD = "sudo -u root -g IHME-SA rsync"
CONFIG_FILE = os.path.join(os.environ["HOME"], ".rsync_nanny.ini")
#CONFIG_FILE = ".rsync_nanny.ini"
ROOT_PATH = "/qumulo"

#TODO Need to error on missing .ini file
#TODO Need to error on invalid cluster
#TODO Need to error on invalid bucket dir
#TODO error on *-nodes > total node count
#TODO turn init_nodes into a function
#TODO maybe add a flag to take last two dirs to name log files?
#TODO move arg checking into the "entry point" part
#TODO maybe add options for arbitrary dest path?
#TODO change get_node to pick a random, least-used node
#TODO tidy up the help section

def main():
    nodes = defaultdict(dict)

    args = get_args()

    #??? Should I assemble a parameter tuple/list and pass it instead?
    if args.ini:
        rsync_nanny(args, args.ini)
    else:
        rsync_nanny(args)

    #TODO needs to allow for specifying .ini file
    rsync_nanny(options)

def get_args():
    parser = argparse.ArgumentParser(description="Generate rsync command lines based on file lists (buckets) and multiple mount points for the same shares to load balance.")
    parser.add_argument("src_cluster", metavar="<source cluster>", help="source Qumulo cluster name")
    parser.add_argument("dest", metavar="<destination>", help="destination storage")
    parser.add_argument("src_path", metavar="<path>", help="path on source cluster that contains entries in the buckets")
    parser.add_argument("--dst-path", "-d",  metavar="<path>", help="path on destination cluster")
    parser.add_argument("--bucket-dir", "-b", metavar="<dir>", help="directory containing buckets")
    parser.add_argument("--src-node", "--sn", default=0, type=int, metavar="<node id>", help="starting node id for the source")
    parser.add_argument("--dst-node", "--dn", default=0, type=int, metavar="<node id>", help="starting node id for the destination")
    parser.add_argument("--src-init", "--si", default=1, type=int, metavar="<int>", help="padding value (requires --src-node")
    parser.add_argument("--dst-init", "--di", default=1, type=int, metavar="<int>", help="padding value (requires --src-node")
    parser.add_argument("--test", dest="test", action ="store_true", help="add the --dry-run flag to rsync")

    args = parser.parse_args()

    if not args.src_node and args.src_init:
        parser.error("--src-init requires --src-node")
    if not args.dst_node and args.dst_init:
        parser.error("--dst-init requires --dst-node")

    #strip leading / or os.path.join will not work
    args.src_path = args.src_path.lstrip(os.sep)

    if args.dst_path is None:
        args.dst_path = args.src_path

    return args

def get_config(config_file):
    config = configparser.ConfigParser()
    config.read(config_file)
    return config

def rsync_nanny(options, config_file = None, configuration = None):
    "This is the entry point when called as a function and what the args get passed to when executed from the command line"
    "options - an argsparse object"
    "configuration (not implemented) - optional a configparser object to override the .ini"
    "config_file (not implemented) - optional file path to override the default .ini file"

    if not config_file:
        config_file = CONFIG_FILE

    if not configuration:
        configuration = get_config(config_file)

    state = initialize_state(options, configuration)

    #launch rsyncs


def initialize_state(options, configuration)
    "Initializes the state object for this run"
    "OLD CODE"

    rsync_opts = config["rsync"]["opts"].split(" ")

    if args.test:
        rsync_opts.append("--dry-run")

    for node_id in range(1, int(config[args.src_cluster]['nodes']) + 1):
        if node_id < args.src_node:
            init = args.src_init
        else:
            init = 0
        nodes[args.src_cluster]["node_{:02}".format(node_id)] = init

    if config[args.dest]['type'] == "qumulo":
        for node_id in range(1, int(config[args.dest]['nodes']) + 1):
            if node_id < args.dst_node:
                init = args.dst_init
            else:
                init = 0
            nodes[args.dest]["node_{:02}".format(node_id)] = init

    if args.bucket_dir:
        bucket_list = sorted(os.listdir(args.bucket_dir))
        while bucket_list:
            src_directory = assemble_dir(args.src_cluster, args.src_path, nodes)
            if config[args.dest]['type'] == "qumulo":
                dst_directory = assemble_dir(args.dest, args.dst_path, nodes)
            elif config[args.dest]['type'] == "path":
                dst_directory = os.path.normpath(os.path.join(config[args.dest]['path'], args.dst_path))

            bucket_file = get_bucket(args.bucket_dir, bucket_list)
            
            launch_rsync(rsync_opts, src_directory, dst_directory, bucket_file)
    else:
        src_directory = os.path.normpath(assemble_dir(args.src_cluster, args.src_path, nodes)) + "/"
        if config[args.dest]['type'] == "qumulo":
            dst_directory = assemble_dir(args.dest, args.dst_path, nodes)
        elif config[args.dest]['type'] == "path":
            dst_directory = os.path.normpath(os.path.join(config[args.dest]['path'], args.dst_path))

def get_node(nodes, cluster):
    nodes = nodes[cluster]
    node = sorted(sorted(nodes.keys()), key = lambda k:nodes[k])[0]
    
    # incrementing the node use count here though that's dirty
    nodes[node] += 1

    return node

def assemble_dir(cluster, path, nodes):
    node = get_node(nodes, cluster)
    directory = os.path.join(ROOT_PATH, cluster, node, path)
    directory = os.path.normpath(directory)
    
    return directory

def get_bucket(bucket_dir, bucket_list):
    bucket_file = os.path.join(bucket_dir, bucket_list.pop(0))
    return bucket_file

def launch_rsync(rsync_opts, src_dir, dst_dir, bucket_file = None):
    if bucket_file:
        # now I am going to developer hell
        file_base = os.path.basename(bucket_file)
        log_file = file_base + ".log"
        out_file = file_base + ".out"

        command_line = [RSYNC_CMD] + rsync_opts + ["--files-from", bucket_file, "--log-file", log_file, src_dir, dst_dir + "/", ">", out_file, "2>&1", "&"]
    else:
        file_base = os.path.basename(os.path.normpath(src_dir))
        log_file = file_base + ".log"
        out_file = file_base + ".out"

        command_line = [RSYNC_CMD] + rsync_opts + ["--log-file", log_file, src_dir, dst_dir + "/", ">", out_file, "2>&1", "&"]

    print(" ".join(command_line))

if __name__ == "__main__":
    main()
