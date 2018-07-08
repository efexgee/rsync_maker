#!/usr/bin/env python3

import argparse
import configparser
import os
from random import choice
from time import sleep
from subprocess import Popen, DEVNULL

    
RSYNC_CMD = "sudo -u root -g IHME-SA rsync"
CONFIG_FILE = os.path.join(os.environ["HOME"], ".rsync_nanny.ini")
ROOT_PATH = "/qumulo"
TIMEOUT = 1

### Need to error on missing .ini file
### Need to error on invalid cluster
### Need to error on invalid bucket dir
### Need to add --delete option
### "rename" all the args. variables?
###rename assemble_dir to reflect that nodes is dynamic
### read TIMEOUT from .ini
### leading slash on dir argument is a problem?
### add trailing slash on dest dir?
### don't read directories in the buckets dir

def main():
    nodes = {}

    args = get_args()
    #print(args)

    bucket_list = sorted(os.listdir(args.bucket_dir))
    print("Processing", len(bucket_list), "buckets with", args.rsyncs, "parallel rsyncs")

    #print(bucket_list)

    config = get_config(CONFIG_FILE)
    #print(config.sections())
    #print(config["rsync"]["opts"])

    rsync_opts = config["rsync"]["opts"].split(" ")

    # initialize node lists for the Qumulo cluster based on the .ini file
    nodes[args.src_cluster] = {"node_{:02}".format(i):0 for i in range(1, int(config[args.src_cluster]['nodes']) + 1)}
    nodes[args.dst_cluster] = {"node_{:02}".format(i):0 for i in range(1, int(config[args.dst_cluster]['nodes']) + 1)}

    #print(nodes)


    #print(src_directory)
    #print(dst_directory)

    # The nanny loop
    # Not the actual loop, but the actual working part of the script
    rsync_jobs = []


    while True:
        new_rsync_jobs = []

        # check the state of all the tracked rsyncs
        for job in rsync_jobs:
            if job.poll() is not None:
                print("Job complete:", job.pid, ":", job.args, "(", job.returncode, ")")
                #print("Job complete:", job.args[6], "(", job.returncode, ")")
                ### need to decrement the node counts
                ### that means jobs need to have meta info attached
            else:
                # collect rsyncs which are still running to make a new tracked rsyncs list
                new_rsync_jobs.append(job)

        # update the rsync list to contain only jobs that are still running
        ### why am I not deleting the jobs from the list? not possible? timing issue?
        rsync_jobs = new_rsync_jobs

        # launch rsyncs until we hit the max job limit
        while len(rsync_jobs) < args.rsyncs and bucket_list:
            src_directory = assemble_dir(args.src_cluster, args.src_path, nodes)
            dst_directory = assemble_dir(args.dst_cluster, args.dst_path, nodes)

            bucket_file = get_bucket(args.bucket_dir, bucket_list)

            rsync_jobs.append(launch_rsync(args.test, rsync_opts, src_directory, dst_directory, bucket_file))
            print("Launched job", len(rsync_jobs), "(PID ",rsync_jobs[-1].pid, ")")

        if not bucket_list and not rsync_jobs:
            # stop main loop only if there are no more things to do and
            # no more running rsyncs
            print("done and done")
            break

        # since the rsyncs run for a long time, we don't need to check very often
        ### the timeout could be set based on rsync run times, but I don't think it's
        ### that expensive to run every minute or so; lost rsync time is more costly
        print("Waiting for", TIMEOUT, "seconds... (", len(rsync_jobs),"rsyncs still running)")
        sleep(TIMEOUT)

    # out of the main loop, so we're done
    print("Done.")

def get_args():
    # parse the command-line arguments
    ### needs to be updated from rsync_maker.py
    parser = argparse.ArgumentParser(description="This tells you what this thing does")
    parser.add_argument("src_cluster", metavar="source cluster", help="source cluster")
    parser.add_argument("dst_cluster", metavar="dest cluster", help="destination cluster")
    parser.add_argument("src_path", metavar="path", help="source and dest path unless -d")
    parser.add_argument("bucket_dir", metavar="bucket dir", help="directory containing buckets")
    parser.add_argument("--dst_path", metavar="dest path", help="specify dest path if paths are diff")
    parser.add_argument("--test", dest="test", action ="store_true", help="don't do anything")
    parser.add_argument("--rsyncs", "-r", default=1, type=int, metavar="<int>", help="number of parallel rsyncs (default = 1)")

    args = parser.parse_args()

    # remove leading / or os.path.join will not work
    args.src_path = args.src_path.lstrip(os.sep)

    if args.dst_path is None:
        args.dst_path = args.src_path
    else:
        args.dst_path = args.dst_path.lstrip(os.sep)

    return args

def get_config(config_file):
    # read the .ini file
    config = configparser.ConfigParser()
    config.read(config_file)
    return config

def make_commandline():
    # assembled the command line to execute (list)
    #
    # needs:
    #   configuration (this invocation of the script)
    #   settings (for the current "task")
    #   node object
    # returns:
    #   command line (list)
    #   node object (modified)
    pass

def get_node(nodes, cluster):
    # find the next node to use for a cluster
    nodes = nodes[cluster]
    node = choice([node_id for node_id in nodes.keys() if nodes[node_id] == min(nodes.values())])
    
    # incrementing the node use count here though that's dirty
    ### I don't like doing this here, but the scope is getting too confusing
    nodes[node] += 1

    return node

def assemble_dir(cluster, path, nodes):
    # assemble a path incorporating one of the least-used nodes
    node = get_node(nodes, cluster)
    directory = os.path.join(ROOT_PATH, cluster, node, path)
    ### should this be one line?
    directory = os.path.normpath(directory)
    
    return directory

def get_bucket(bucket_dir, bucket_list):
    # assemble path of the next bucket file 
    bucket_file = os.path.join(bucket_dir, bucket_list.pop(0))
    return bucket_file

def launch_rsync(dry_run, rsync_opts, src_dir, dst_dir, bucket_file):
    # now I am going to developer hell
    file_base = os.path.basename(bucket_file)
    log_file = file_base + ".log"
    out_file = file_base + ".out"

    if dry_run:
        rsync_opts.append("--dry-run")

    # importing random here as it's only used in this test command
    from random import random
    command_line = [RSYNC_CMD] + rsync_opts + ["--files-from", bucket_file, "--log-file", log_file, src_dir, dst_dir, ">", out_file, "2>&1"]
    #command_line = ["sleep", str(random() * 6), "&&", "echo", "poop", ">> poop.log"]
    command_line = ["sleep", str(random() * 6), "&&", "echo '"] + command_line + ["'", ">> poop.log"]

    command_line = " ".join(command_line)

    return Popen(command_line, stdout=DEVNULL, shell=True)

def check_for_completed(rsync_list):
    ### I don't know why this is here
    pass

def ok_to_launch(cur_rsyncs, max_rsyncs, bucket_list):
    # this is here to allow for other checks to be added, such as network saturation
    # or Qumulo load, etc.

    #if cur_rsyncs < max_rsyncs

    #if bucket_list is not empty
    pass
    

if __name__ == "__main__":
    main()
