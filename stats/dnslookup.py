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


# Main
if len(sys.argv) != 5:
    print("Usage: " + sys.argv[0] + "nb_trials ip2as.csv publicsuffix.dat duplicate_list target_domain_list result_file")
    exit(1)

ip2as_file = sys.argv[2]
public_suffix_file = sys.argv[3]
domain_list_file = sys.argv[4]
new_result_file = sys.argv[5]

ps = pubsuffix.public_suffix()
if not ps.load_file(public_suffix_file):
    print("Could not load the suffixes")
    exit(1)

i2a = ip2as.ip2as_table()
if i2a.load(ip2as_file):
    print("Loaded i2a table of length: " + str(len(i2a.table)))
else:
    print("Could not load <" + ip2as_file + ">")

million = []
million_ranges = [0,100]
million_range_done = [True, False]
million_rank = 0
current_range = 1
try: 
    for line in open(domain_list_file, "rt", encoding="utf-8"):
        million.append[line.strip()]
        million_rank += 1
        if million_rank > million_ranges[current_range]:
            million_ranges.append(million_ranges[current_range]*10)
            million_range_done.append(False)
            current_range += 1
    if million_ranges[current_range] > len(million):
        million_ranges[current_range] = len(million)
    print("Targeting " + str(len(million)) + " domain names.")
except Exception as e:
    traceback.print_exc()
    print("Cannot read file <" + domain_list_file  + ">\nException: " + str(e))
    print("Giving up");
    exit(1)

already_found = dict()
try:
    for line in open(result_file, "rt", encoding="utf-8"):
        js_line = line.strip()
        if len(js_line) > 0:
            w = dnslook.dnslook()
            if w.from_json(js_line):
                already_found[w.domain] = 1
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

# Once everything is done, start getting the requested number of new names
# The names are picked at random from five zones in the million names list

nb_tried = 0
current_range = 1
while nb_assessed < nb_trials and current_range > 0:
    # Find a name at random using the 100/1000/10000/100000 split
    x = ""
    n = random.randrange(million_ranges[current_range-1],million_ranges[current_range])
    n0 = n
    while True:
        if not million[n] in already_found:
            x = million[n]
            break
        n += 1
        if n >= million_ranges[current_range]:
            n = million_ranges[current_range-1]
        if n == n0:
            # the entire range is done. No name available
            million_range_done[current_range] = True
            break
    # progress the range
    next_range = current_range+1
    while True:
        if next_range > len(million_ranges):
            next_range = 1
        if not million_range_done[next_range]:
            current_range = next_range
            break
        elif next_range == current_range:
            # full loop, all ranges done
            current_range = -1
    


    x = million[n]


nb_lines = 0
nb_domains = 0
try:
    f_out = open(new_result_file, "wt")
    try: 
        for line in open(domain_list_file , "rt"):
            nb_lines += 1 
            d = dnslook.dnslook()
            if d.from_json(line):
                if len(d.ns) == 0:
                    print("Correcting: " + d.domain)
                    d.get_ns()
                    # if that was successful, try getting an IP address
                    if len(d.ns) > 0 and len(d.ip) == 0:
                        d.get_a()
                    # if that was successful, try filling other missing data
                    # if not, don't, because that would be a waste of time
                    if len(d.ip) > 0:
                        if len(d.ipv6) == 0:
                            d.get_aaaa()
                        if len(d.cname) == 0:
                            d.get_cname()
                if d.server == "":
                    d.get_server(ps)
                if d.as_number == 0:
                    d.get_asn(i2a)
                f_out.write(d.to_json() + "\n")
                nb_domains += 1
        print("All done")
    except Exception as e:
        traceback.print_exc()
        print("Cannot read file <" + domain_list_file  + ">\nException: " + str(e))
        print("Giving up");
    f_out.close()
except Exception as e:
    traceback.print_exc()
    print("Cannot create file <" + new_result_file  + ">\nException: " + str(e))
    print("Giving up");

print("nb_lines: " + str(nb_lines))
print("nb_domains: " + str(nb_domains))
