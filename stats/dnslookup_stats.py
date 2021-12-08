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
import zoneparser
import time
import concurrent.futures
import os

def load_dns_file(dns_json):
    stats = []
    loaded = 0
    for line in open(dns_json, "rt"):
        dns_look = dnslook.dnslook()
        dns_look.from_json(line)
        stats.append(dns_look)
        loaded += 1
        if loaded%100 == 0:
            sys.stdout.write(".")
            sys.stdout.flush()
    return stats

def load_million_file(million_file):
    millions = dict()
    rank = 0
    for line in open(million_file, "rt"):
        dict[line.strip()] = rank
        rank += 1
# Main

million_file = sys.argv[2]
public_suffix_file = sys.argv[3]
dups_file = sys.argv[4]
result_file = sys.argv[5]
stats = load_dns_file(sys.argv[1])
print("\nLoaded " + str(len(stats)) + " lines.")
ns_dict = dict()


ps = pubsuffix.public_suffix()
if not ps.load_file(public_suffix_file):
    print("Could not load the suffixes")
    exit(1)
print("\nLoaded " + str(len(ps.table)) + " suffixes.")
zp = zoneparser.zone_parser2(ps)
zp.load_dups(dups_file)

print("loaded the dependencies")

for dns_lookup in stats:
    ns_this_name = dict()
    for ns_name in dns_lookup.ns:
        ns_suffix = zoneparser.extract_server_suffix(ns_name, ps, zp.dups);
        if ns_suffix in ns_this_name:
            ns_this_name[ns_suffix] += 1
        else:
            ns_this_name[ns_suffix] = 1
    for ns_suffix in ns_this_name:
        if ns_suffix in ns_dict:
            ns_dict[ns_suffix] += 1
        else:
            ns_dict[ns_suffix] = 1
print("processed the names")


with open(result_file,"wt") as f:
    f.write("name, refs\n");
    for ns_name in ns_dict:
        f.write(ns_name + "," + str(ns_dict[ns_name]) + "\n")
        
print("saved " + str(len(ns_dict)) + " services.")

