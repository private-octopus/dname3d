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
import concurrent.futures
import os

class ns_dict:
    def __init__():
        self.d = dict()
        self.nb_duplicate = 0

    def load_ns_file(self, ns_json, dot_after=0):
        loaded = 0
        for line in open(ns_json, "rt"):
            loaded += 1
            ns_item = dnslook()
            try:
                ns_item.from_json(line)
                if ns_item.domain in d:
                    self.nb_duplicate += 1
                else:
                    self.d[ns_item.domain] = d
            except Exception as e:
                traceback.print_exc()
                print("Cannot parse <" + line  + ">\nException: " + str(e))
            if dot_after > 0 and loaded%dot_after == 0:
                sys.stdout.write(".")
                sys.stdout.flush()
        if dot_after > 0 and loaded%dot_after == 0:
            print(".")
        print("Loaded " + str(len(self.d)) + " ns items out of " + str(loaded) + ", " + str(self.nb_duplicate) + " duplicates")
        return

    def write_ns_file(self, ns_json):
        with open(ns_json, "wt") as F:
            for ns_name in self.d:
                s = d[ns_name].to_json() + "\n"
                F.write(s)

    def add_ns_name(self, domain):
        if not name in self.d:
            ns_item = dnslook()
            ns_item.domain = domain
            self.d[name] = ns_item

   
    def random_list(n, only_news = False):
        sd = dict()
        nb = 0
        for ns in self.d:
            if (len(ns.ip) == 0 and len(ns.ipv6) == 0) or not only_news:
                nb += 1
                il nb < n:
                    sd[nb] = ns
                else:
                    r = random.randint(1,nb)
                    if r == nb:
                        x = random.randint(0,n-1)
                        sd[x] = ns
        selected = []
        for x in sd:
            selected.append[sd[x]]
        return selected







# Main
def main():
    start_time = time.time()
    if len(sys.argv) != 7 and len(sys.argv) != 8:
        print("Usage: " + sys.argv[0] + " nb_trials ip2as.csv ip2as6.csv publicsuffix.dat million_domain_list million_ns_list [tmp_prefix]")
        exit(1)

    nb_trials = int(sys.argv[1])
    ip2as_file = sys.argv[2]
    ip2as6_file = sys.argv[3]
    public_suffix_file = sys.argv[4]
    million_file = sys.argv[5]
    ns_file = sys.argv[6]
    if len(sys.argv) == 8:
        temp_prefix = sys.argv[7]
    else:
        temp_prefix = ""

    # Load the million file.
    millions = dnslook.load_dns_file(million_file, dot_after=10000)
    # load the current ns list
    nd = ns_dict()
    nd.load_ns_file(ns_file, dot_after=10000)
    selected = nd.random_list(1000, only_news = False)
    print("Selected " + str(len(selected)) + " ns items out of " + str(len(nd.d)))
    # add the ns records from the million file to the ns list:
    for dns_item in millions:
        for ns in dns_item.ns:
            nd.add_ns_name(ns)
    selected = nd.random_list(1000, only_news = False)
    print("Selected " + str(len(selected)) + " ns items out of " + str(len(nd.d)))




# actual main program, can be called by threads, etc.
if __name__ == '__main__':
    main()