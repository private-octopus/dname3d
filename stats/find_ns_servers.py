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
import zoneparser
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

class key_range_item:
    def __init__(self, key, rng, weight):
        self.key = key
        self.rng = rng
        self.weight = weight

class key_weights:
    def __init__(self):
        self.weight = dict()
        self.total = [0, 0, 0, 0, 0, 0, 0]
        self.nb_names = [0, 0, 0, 0, 0, 0, 0]

    # add a vector of weights for a set of keys
    def add_key_weight(self, key_set, key_weight):
        w = 1.0
        if len(key_set) > 1:
            w /= len(key_set)
        for key in key_set:
            if not key in self.weight:
                self.weight[key] = [ 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0 ]
            for i in range(0,7):
                if len(key_weight) > 1:
                    self.weight[key][i] += key_weight[i]*w

    # add one domain name in million range rng
    def add_names(self, key_set, rng):
        w = [ 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0 ]
        if rng >= 0 and rng < 7:
            w[rng] = 1.0
            self.add_key_weight(key_set, w)
            self.nb_names[rng] += 1
            self.total[rng] += len(key_set)
        else:
            print("RNG = " + str(rng))

    # we have weights per key. we define top keys as keys that
    # have either a max value in a specific column, or a max value
    # for the sum of columns 0 to 4 (100, 1000, 10000, 100000, 1M)
    def get_sorted_list(self, rng):
        key_list = []
        for key in self.weight:
            w = 0
            weights = self.weight[key]
            if rng >= 0 and rng < len(weights):
                w = weights[rng]
            elif rng < 0:
                for i in range(0,6):
                    if len(weights) > i:
                        w += weights[i]
            if w > 0:
                key_list.append(key_range_item(key, rng, w))

        sorted_list = sorted(key_list, key=lambda item: item.weight, reverse=True)
        return sorted_list

def key_range_item_to_M9(kri, F):
    # First compute total and average
    # then compute top50, top 90
    # then write the "top N" list
    pass

# Main
def main():
    start_time = time.time()
    if len(sys.argv) != 9 and len(sys.argv) != 10:
        print("Usage: " + sys.argv[0] + " nb_trials ip2as.csv ip2as6.csv publicsuffix.dat dups asn_file million_domain_list million_ns_list [tmp_prefix]")
        exit(1)
    try:
        nb_trials = int(sys.argv[1])
    except Exception as e:
        print("Incorect nb_trials value (" + sys.argv[1] + "), exception: " + str(e))
        exit(1)
    ip2as_file = sys.argv[2]
    ip2as6_file = sys.argv[3]
    public_suffix_file = sys.argv[4]
    dups_file = sys.argv[5]
    asn_file = sys.argv[6]
    million_file = sys.argv[7]
    ns_file = sys.argv[8]
    if len(sys.argv) == 10:
        temp_prefix = sys.argv[9]
    else:
        temp_prefix = ""

    ps = pubsuffix.public_suffix()
    if not ps.load_file(public_suffix_file):
        print("Could not load the suffixes")
        exit(1)
    
    zp = zoneparser.zone_parser2(ps)
    zp.load_dups(dups_file)
    # get the AS names
    asns = ip2as.asname()
    if not asns.load(asn_file):
        exit(-1)
    asn_ag = ip2as.aggregated_asn()
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

    print("NS list has " + str(len(nd.d)) + " entries, scanning millions.")
    # Adding NS records from million list
    for dnsl in millions:
        for ns in dnsl.ns:
            nd.add_ns_name(ns)
    print("NS list has " + str(len(nd.d)) + " entries")
    
    targets = nd.random_list(nb_trials, only_news=True)
    print("Selected " + str(len(targets)) + " targets out of " + str(len(nd.d)))
    
    # add the ns records from the million file to the ns list:
    stats = [ 0, 0, 0, 0, 0, 0, 0]
    dl = dns_bucket.bucket_list(nd.d, targets, ps, i2a, i2a6, temp_prefix, "_ns.csv", "_stats.csv")
    dl.run()
    
    nd.save_ns_file(ns_file)
    # tabulate by AS, NS, etc.
    ns_weights = key_weights()
    suffix_weights = key_weights()
    as_weights = key_weights()
    ns_as_weight2 = key_weights()
    for dns_item in millions:
        ns_suffixes = set()
        as_numbers = set()
        ns_as_numbers = set()
        for ns in dns_item.ns:
            nd.add_ns_name(ns)
            ns_suffix = zoneparser.extract_server_suffix(ns, ps, zp.dups)
            ns_suffixes.add(ns_suffix)
            for asn in nd.d[ns].ases:
                ns_as_numbers.add(asn_ag.get_asn(asn))
        ns_weights.add_names(dns_item.ns, dns_item.million_range)
        suffix_weights.add_names(ns_suffixes, dns_item.million_range)
        ns_as_weight2.add_names(ns_as_numbers, dns_item.million_range)
        for asn in dns_item.ases:
            as_numbers.add(asn_ag.get_asn(asn))
        as_weights.add_names(as_numbers, dns_item.million_range)
    print("After loading from millions, NS weights has " + str(len(ns_weights.weight)) + " entries")
    print("After loading from millions, Suffix weights has " + str(len(suffix_weights.weight)) + " entries")
    print("After loading from millions, AS weights has " + str(len(as_weights.weight)) + " entries")
    print("After loading from NS, NS_AS2 weights has " + str(len(ns_as_weight2.weight)) + " entries")
    ns_as_weights = key_weights()
    for ns in ns_weights.weight:
        if ns in nd.d:
            as_numbers = set()
            for asn in nd.d[ns].ases:
                as_numbers.add(asn_ag.get_asn(asn))
            ns_as_weights.add_key_weight(as_numbers, ns_weights.weight[ns])
    print("After loading from NS, NS_AS weights has " + str(len(ns_as_weights.weight)) + " entries")

    # try sorting
    top_ns = ns_weights.get_sorted_list(-1)
    print("Top_ns has : " + str(len(top_ns)))
    nb_top = 0
    for kri in top_ns:
        nb_top += 1
        print(str(nb_top) + ": " + str(kri.key) + "," + str(kri.weight))
        if nb_top >= 10:
            break;
    top_suffix = suffix_weights.get_sorted_list(-1)
    print("top_suffix has : " + str(len(top_suffix)))
    nb_top = 0
    for kri in top_suffix:
        nb_top += 1
        print(str(nb_top) + ": " + str(kri.key) + "," + str(kri.weight))
        if nb_top >= 10:
            break;
    top_as = as_weights.get_sorted_list(-1)
    print("Top_as has : " + str(len(top_as)))
    nb_top = 0
    for kri in top_as:
        nb_top += 1
        print(str(nb_top) + ": "  + str(kri.key) + ", " + asns.name(kri.key) + ", " + str(kri.weight))
        if nb_top >= 10:
            break;
    top_ns_as = ns_as_weights.get_sorted_list(-1)
    print("Top_ns_as has : " + str(len(top_ns_as)))
    nb_top = 0
    for kri in top_ns_as:
        nb_top += 1
        print(str(nb_top) + ": " + str(kri.key)  + ", " + asns.name(kri.key) + ", " + str(kri.weight))
        if nb_top >= 10:
            break
    top_ns_as2 = ns_as_weight2.get_sorted_list(-1)
    print("Top_ns_as has : " + str(len(top_ns_as)))
    nb_top = 0
    for kri in top_ns_as2:
        nb_top += 1
        print(str(nb_top) + ": " + str(kri.key)  + ", " + asns.name(kri.key) + ", " + str(kri.weight))
        if nb_top >= 10:
            break

# actual main program, can be called by threads, etc.
if __name__ == '__main__':
    main()