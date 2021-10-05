#!/usr/bin/python
# coding=utf-8
#
# This script collects data on sites listed in a master file such as the "magnificent million".
# The strategy is to pick at random names of small and large files, get the data, and add it
# to the result file. Running the program in the background will eventually accumulate enough
# data to do meaning ful statistics.

import sys
import dns.resolver
import json
import traceback
import pubsuffix
import ip2as
import dnslook
import random
import million_random
import time


# Main
start_time = time.time()
if len(sys.argv) != 6:
    print("Usage: " + sys.argv[0] + "nb_trials ip2as.csv publicsuffix.dat million_domain_list result_file")
    exit(1)
nb_trials = int(sys.argv[1])
ip2as_file = sys.argv[2]
public_suffix_file = sys.argv[3]
million_file = sys.argv[4]
result_file = sys.argv[5]

ps = pubsuffix.public_suffix()
if not ps.load_file(public_suffix_file):
    print("Could not load the suffixes")
    exit(1)

i2a = ip2as.ip2as_table()
if i2a.load(ip2as_file):
    print("Loaded i2a table of length: " + str(len(i2a.table)))
else:
    print("Could not load <" + ip2as_file + ">")

# TODO: start million random

mr = million_random.million_random(100, 10)
# Read the names already in the result file, remove them from consideration.
try:
    for line in open(result_file, "rt", encoding="utf-8"):
        js_line = line.strip()
        if len(js_line) > 0:
            w = dnslook.dnslook()
            if w.from_json(js_line):
                 mr.set_already_processed(w.domain)
            else:
                print("Cannot parse result line " + line.strip())
                print("Closing " + result_file + " and exiting.")
                exit(1)
except FileNotFoundError:
    # doesn't exist
    print("File " + result_file + " will be created.")
except Exception as e:
    traceback.print_exc()
    print("Cannot load file <" + result_file  + ">\nException: " + str(e))
    print("Giving up");
    exit(1)
# Load the million file. The loading process will not load the names 
# marked as already processed in the previous loop.
mr.load(million_file)
# Once everything is ready, start getting the requested number of new names
# The names are picked at random from five zones in the million names list
# as encoded in the "million_random" class
pick_start = time.time()
print("Ready after " + str(pick_start - start_time))
nb_assessed = 0
stat_name = ["a", "aaaa", "ns", "cname", "server", "asn"]
stats = []
for x in range(0,6):
    stats.append(0)
try:
    with open(result_file, "at") as f_out:
        while nb_assessed < nb_trials:
            domain = mr.random_pick()
            if domain == "":
                break
            try:
                d = dnslook.dnslook()
                # Get the name servers, etc. 
                d.get_domain_data(domain, ps, i2a, stats)
                # Write the json line in the result file.
                f_out.write(d.to_json() + "\n")
                nb_assessed += 1
                mr.mark_read(domain)
            except Exception as e:
                traceback.print_exc()
                print("Cannot assess domain <" + domain  + ">\nException: " + str(e))
                print("Giving up");
except Exception as e:
    traceback.print_exc()
    print("Cannot create file <" + result_file  + ">\nException: " + str(e))
    print("Giving up");
pick_end = time.time()
print("Assessed " + str(nb_assessed) + " domains in " + str(pick_end - pick_start))
for x in range(0,6):
    print("Time " + stat_name[x] + ": " + str(stats[x]/nb_assessed))

