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
import dns_bucket
import time
import concurrent.futures
import os

class ns_dict:
    def __init__(self):
        self.d = dict()
        self.nb_duplicate = 0

    def load_ns_file(self, ns_json, dot_after=0):
        loaded = 0
        for line in open(ns_json, "rt"):
            loaded += 1
            ns_item = dnslook.dnslook()
            try:
                ns_item.from_json(line)
                if ns_item.domain in self.d:
                    self.nb_duplicate += 1
                else:
                    self.d[ns_item.domain] = ns_item
            except Exception as e:
                traceback.print_exc()
                print("Cannot parse <" + line  + ">\nException: " + str(e))
            if dot_after > 0 and loaded%dot_after == 0:
                sys.stdout.write(".")
                sys.stdout.flush()
        if dot_after > 0:
            print("")
        print("Loaded " + str(len(self.d)) + " ns items out of " + str(loaded) + ", " + str(self.nb_duplicate) + " duplicates")
        return

    def save_ns_file(self, ns_json, dot_after=0):
        saved = 0
        with open(ns_json, "wt") as F:
            for ns in self.d:
                ns_item = self.d[ns]
                if ns_item.nb_queries > 0:
                    js = ns_item.to_json()
                    F.write(js + "\n")
                    saved += 1
                    if dot_after > 0 and saved%dot_after == 0:
                        sys.stdout.write(".")
                        sys.stdout.flush()
        if dot_after > 0:
            print("")
        print("Saved " + str(saved) + " ns items out of " + str(len(self.d)))
        return

    def add_ns_name(self, domain):
        if not domain in self.d:
            ns_item = dnslook.dnslook()
            ns_item.domain = domain
            self.d[domain] = ns_item

    def random_list(self, n, only_news=False):
        sd = dict()
        nb = 0
        for ns in self.d:
            ns_item = self.d[ns]
            if not only_news or (ns_item.nb_queries == 0) or (ns_item.dns_timeout > 0 and ns_item.nb_queries < 3):
                nb += 1
                if nb <= n:
                    sd[nb] = ns_item
                else:
                    r = random.randint(1,nb)
                    if r == nb:
                        x = random.randint(0,n-1)
                        sd[x] = ns_item
        targets = []
        for x in sd:
            target = million_random.million_target(sd[x].domain, sd[x].million_rank, sd[x].million_range)
            targets.append(target)
        return targets

    def get_data(self, targets, ps, i2a, i2a6, stats):
        for target in selected:
            ns_item = self.d[target.domain]
            if ns_item.nb_queries == 0:
                ns_item.get_domain_data(target.domain, ps, i2a, i2a6, stats)
            else:
                ns_item.retry_domain_data(ps, i2a, i2a6, stats)

class ns_stats_by_range:
    def __init__(self):
        self.table = dict()
        self.sum_weights = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
            
class ns_stats_by_key:
    def __init__(self, key_type):
        self.key_type = key_type
        self.table = dict()
        self.sum_weights = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]

    def add_key_weights(self, keys, weights):
        if len(keys) > 0:
            wkey = 1.0/len(keys)
            for key in keys:
                if not key in self.table:
                    self.table[key] = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
                for x in range(0,7):
                    self.table[key][x] += weights[x]*wkey
                    self.sum_weights[x] += weight[x]*wkey

    def add_domain_weights(self, ns_set, rng):
        if rng > 0 and rng < 7:
            weight = 1.0
            if len(ns_set) > 1:
                weight /= len(ns_set)
            for ns in ns_set:
                if not ns in self.table:
                    self.table[ns] = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
                self.table[ns][rng] += weight
                self.sum_weights[rng] += weight






# Main
def main():
    start_time = time.time()
    if len(sys.argv) != 7 and len(sys.argv) != 8:
        print("Usage: " + sys.argv[0] + " nb_trials ip2as.csv ip2as6.csv publicsuffix.dat million_domain_list million_ns_list [tmp_prefix]")
        exit(1)
    try:
        nb_trials = int(sys.argv[1])
    except Exception as e:
        print("Incorect nb_trials value (" + sys.argv[1] + "), exception: " + str(e))
        exit(1)
    ip2as_file = sys.argv[2]
    ip2as6_file = sys.argv[3]
    public_suffix_file = sys.argv[4]
    million_file = sys.argv[5]
    ns_file = sys.argv[6]
    if len(sys.argv) == 8:
        temp_prefix = sys.argv[7]
    else:
        temp_prefix = ""

    ps = pubsuffix.public_suffix()
    if not ps.load_file(public_suffix_file):
        print("Could not load the suffixes")
        exit(1)

    i2a = ip2as.load_ip2as(ip2as_file)
    i2a6 = ip2as.load_ip2as(ip2as6_file)

    # Load the million file.
    millions = dnslook.load_dns_file(million_file, dot_after=10000)
    print("\nLoaded " + str(len(millions)) + " domains from million file.")
    # load the current ns list
    nd = ns_dict()
    try:
        nd.load_ns_file(ns_file, dot_after=10000)
    except Exception as e:
        print("Could not load " + ns_file + ", exception: " + str(e))
    print("NS list has " + str(len(nd.d)) + " entries")
    # add the ns records from the million file to the ns list:
    for dns_item in millions:
        for ns in dns_item.ns:
            nd.add_ns_name(ns)
    print("After loading from millions, NS list has " + str(len(nd.d)) + " entries")
    targets = nd.random_list(nb_trials, only_news=True)
    print("Selected " + str(len(targets)) + " targets out of " + str(len(nd.d)))
    stats = [ 0, 0, 0, 0, 0, 0, 0]
    # nd.get_data(selected, ps, i2a, i2a6, stats)
    dl = dns_bucket.bucket_list(nd.d, targets, ps, i2a, i2a6, temp_prefix, "_ns.csv", "_stats.csv")
    dl.run()

    nd.save_ns_file(ns_file)



# actual main program, can be called by threads, etc.
if __name__ == '__main__':
    main()