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


def key_list_to_M9(key_list, m9date, metric, key_per_name, top_set, F):
    total_weight = 0
    # First compute total and average
    for kri in key_list:
        total_weight += kri.weight
    if total_weight > 0:
        # then compute top50, top 90
        cumul_weight = 0
        weight50 = 50*total_weight / 100
        weight90 = 90*total_weight / 100
        top50 = 1
        top90 = 1
        for kri in key_list:
            cumul_weight += kri.weight
            if cumul_weight < weight50:
                top50 += 1
            if cumul_weight < weight90:
                top90 += 1
            else:
                break
        # write the values
        F.write(metric + ".1," + m9date +",v2.0, ," + str(key_per_name) + "\n")
        F.write(metric + ".2," + m9date + ",v2.0, ," + str(top50) + "\n")
        F.write(metric + ".3," + m9date + ",v2.0, ," + str(top90) + "\n")
        nb_written = 0
        for kri in key_list:
            fraction =  kri.weight / total_weight
            nb_written += 1
            if nb_written <= 10 or fraction >= 0.005 or kri.key in top_set :
                F.write(metric + ".4," + m9date + ",v2.0, " + str(kri.key) + "," + str(fraction) + "\n")
            if nb_written > 10 and fraction < 0.0005:
                break

class key_weights:
    def __init__(self):
        self.weight = dict()
        self.total = [0, 0, 0, 0, 0, 0, 0]
        self.nb_names = [0, 0, 0, 0, 0, 0, 0]

    # add a vector of weights for a set of keys
    def add_key_weight(self, key_set, key_weight,fixed_weight=False):
        w = 1.0
        if len(key_set) > 1 and not fixed_weight:
            w /= len(key_set)
        for key in key_set:
            if not key in self.weight:
                self.weight[key] = [ 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0 ]
            for i in range(0,7):
                if len(key_weight) > i:
                    self.weight[key][i] += key_weight[i]*w

    # add one domain name in million range rng
    def add_names(self, key_set, rng, fixed_weight=False):
        w = [ 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0 ]
        if rng >= 0 and rng < 7:
            w[rng] = 1.0
            self.add_key_weight(key_set, w, fixed_weight=fixed_weight)
            if len(key_set) > 0:
                self.nb_names[rng] += 1
                self.total[rng] += len(key_set)

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

    # writing the metrics
    def weights_to_m9(self, m9date, metric, m_first, F, init_set=[], write_metric=True, fixed_weight=False):
        key_lists=[]
        top_set = set()
        for key in init_set:
            top_set.add(key)
        for i in range(0,6):
            key_lists.append(self.get_sorted_list(i))
        for i in range(0,6):
            kl = key_lists[i]
            nb = len(kl)
            if nb > 5:
                nb = 5
            for j in range(0,nb):              
                top_set.add(kl[j].key)
        if write_metric:
            for i in range(0,6):
                if self.nb_names[i] > 0:
                    key_per_name = self.total[i]/self.nb_names[i]
                    print(str(metric) + "." + str(m_first + i) + ": " +  str( self.total[i]) + ", " + str(self.nb_names[i]))
                    key_list_to_M9(key_lists[i], m9date, metric + "." + str(m_first + i), key_per_name, top_set, F)
        return top_set


def write_key_range_items(key_list, file_name, key_name, weight_name):
    with open(file_name, "w") as F:
        F.write(key_name + ", " + weight_name + "\n")
        for kri in key_list:
            F.write(str(kri.key) + "," + str(kri.weight) + "\n")
    print("Saved " + str(len(key_list)) + " " + key_name + " in " + file_name)

def print_top_key_range_items(key_list, list_name):
    print(list_name + " has : " + str(len(key_list)))
    nb_top = 0
    for kri in key_list:
        nb_top += 1
        print(str(nb_top) + ": " + str(kri.key) + "," + str(kri.weight))
        if nb_top >= 10:
            break;

def print_top_key_range_asn(key_list, list_name, asns):
    print(list_name + " has : " + str(len(key_list)))
    nb_top = 0
    for kri in key_list:
        nb_top += 1
        print(str(nb_top) + ": "  + str(kri.key) + ", " + asns.name(kri.key) + ", " + str(kri.weight))
        if nb_top >= 10:
            break;

def millions_to_suffix_weights(millions, ps, dups, fixed_weight):
    suffix_weights = key_weights()
    for dns_item in millions:
        ns_suffixes = set()
        for ns in dns_item.ns:
            ns_suffix = zoneparser.extract_server_suffix(ns, ps, dups)
            ns_suffixes.add(ns_suffix)
        suffix_weights.add_names(ns_suffixes, dns_item.million_range, fixed_weight=fixed_weight)
    return suffix_weights

def millions_to_ns_as_weights(millions, nd, asn_ag, fixed_weight):
    ns_as_weights = key_weights()
    for dns_item in millions:
        ns_as_numbers = set()
        for ns in dns_item.ns:
            for asn in nd.d[ns].ases:
                ns_as_numbers.add(asn_ag.get_asn(asn))
        ns_as_weights.add_names(ns_as_numbers, dns_item.million_range, fixed_weight=fixed_weight)
    return ns_as_weights

def millions_to_as_weights(millions, asn_ag, fixed_weight):
    as_weights = key_weights()
    for dns_item in millions:
        as_numbers = set()
        for asn in dns_item.ases:
            as_numbers.add(asn_ag.get_asn(asn))
        as_weights.add_names(as_numbers, dns_item.million_range, fixed_weight=fixed_weight)
    return as_weights

def compute_m9(millions, ps, dups, nd, asn_ag, asns, m9date, fixed_weight, F):
    suffix_weights = millions_to_suffix_weights(millions, ps, dups, fixed_weight)
    suffix_weights.weights_to_m9(m9date, "M9", 1, F)
    as_weights = millions_to_as_weights(millions, asn_ag, fixed_weight)
    top_as = as_weights.weights_to_m9(m9date, "M9", 13, F, write_metric=False)

    ns_as_weights = millions_to_ns_as_weights(millions, nd, asn_ag, fixed_weight)
    top_as = ns_as_weights.weights_to_m9(m9date, "M9", 7, F, top_as)
    as_weights.weights_to_m9(m9date, "M9", 13, F, top_as)
    for asn in top_as:
        F.write("M9.19.1," + m9date + ",v2.0, " + ip2as.asname.clean(asns.name(asn)) + "," + str(asn) + "\n")

def save_m9(millions, ps, dups, nd, asn_ag, asns, m9date, fixed_weight, file_name):
    with open(file_name, "w") as F:
        compute_m9(millions, ps, dups, nd, asn_ag, asns, m9date, fixed_weight, F)


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
    fixed_weight = True
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
    
    # load the current ns list
    nd = ns_dict()
    try:
        nd.load_ns_file(ns_file, dot_after=10000)
    except Exception as e:
        print("Could not load " + ns_file + ", exception: " + str(e))

    print("NS list has " + str(len(nd.d)) + " entries, scanning millions.")

    # parse the million file for additional NS records, but do not
    # actually load it. Loading it would increase the memory footprint
    # of the process by maybe 1GB. Not a problem per se, but we will
    # then fork N processes for the "bucket" evaluation, for a combined
    # memory foot print of maybe 256GB, and that would not to work well.
    dnsl_loaded = 0
    dot_after = 10000
    for line in open(million_file, "rt"):
        dnsl = dnslook.dnslook()
        try:
            dnsl.from_json(line)
            for ns in dnsl.ns:
                nd.add_ns_name(ns)
            dnsl_loaded += 1
        except Exception as e:
            traceback.print_exc()
            print("Cannot parse <" + line  + ">\nException: " + str(e))
        if dot_after > 0 and dnsl_loaded%dot_after == 0:
            sys.stdout.write(".")
            sys.stdout.flush()
    print("\nParsed " + str(dnsl_loaded) + " domains from million file.")
    print("NS list has " + str(len(nd.d)) + " entries")
    
    targets = nd.random_list(nb_trials, only_news=True)
    print("Selected " + str(len(targets)) + " targets out of " + str(len(nd.d)))
    
    # add the ns records from the million file to the ns list:
    stats = [ 0, 0, 0, 0, 0, 0, 0]
    dl = dns_bucket.bucket_list(nd.d, targets, ps, i2a, i2a6, temp_prefix, "_ns.csv", "_stats.csv")
    dl.run()
    
    
    # Load the million file now. Maybe this could be combined with the first pass,
    # to save disk IO, but we need to look at how the million file is used
    # in the next computations first.
    millions = dnslook.load_dns_file(million_file, dot_after=10000)
    print("\nLoaded " + str(len(millions)) + " domains from million file.")

    nd.save_ns_file(ns_file)
    # tabulate by AS, NS, etc.
    ns_weights = key_weights()
    suffix_weights = key_weights()
    as_weights = key_weights()
    ns_as_weight2 = key_weights()
    ns_ip4_weight = key_weights()
    ns_ip6_weight = key_weights()
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
            ns_ip4_weight.add_names(nd.d[ns].ip, dns_item.million_range, fixed_weight=fixed_weight)
            ns_ip6_weight.add_names(nd.d[ns].ipv6, dns_item.million_range, fixed_weight=fixed_weight)
        ns_weights.add_names(dns_item.ns, dns_item.million_range, fixed_weight)
        suffix_weights.add_names(ns_suffixes, dns_item.million_range, fixed_weight)
        ns_as_weight2.add_names(ns_as_numbers, dns_item.million_range, fixed_weight)
        
        for asn in dns_item.ases:
            as_numbers.add(asn_ag.get_asn(asn))
        as_weights.add_names(as_numbers, dns_item.million_range, fixed_weight=fixed_weight)
    print("After loading from millions, NS weights has " + str(len(ns_weights.weight)) + " entries")
    print("After loading from millions, Suffix weights has " + str(len(suffix_weights.weight)) + " entries")
    print("After loading from millions, AS weights has " + str(len(as_weights.weight)) + " entries")
    print("After loading from NS, NS_AS2 weights has " + str(len(ns_as_weight2.weight)) + " entries")
    print("After loading from NS, NS_IP4 weights has " + str(len(ns_ip4_weight.weight)) + " entries")
    print("After loading from NS, NS_IP6 weights has " + str(len(ns_ip4_weight.weight)) + " entries")

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
    print_top_key_range_items(top_ns, "top_ns")
    top_ns4 = ns_weights.get_sorted_list(4)
    print_top_key_range_items(top_ns4, "top_ns4")
    top_suffix = suffix_weights.get_sorted_list(-1)
    print_top_key_range_items(top_suffix, "top_suffix")
    top_as = as_weights.get_sorted_list(-1)
    print_top_key_range_asn(top_as, "top_as", asns)
    top_ns_as = ns_as_weights.get_sorted_list(-1)
    print_top_key_range_asn(top_ns_as, "top_ns_as", asns)
    top_ns_as2 = ns_as_weight2.get_sorted_list(-1)
    print_top_key_range_asn(top_ns_as2, "top_ns_as2", asns)

    top_ns_ip4 = ns_ip4_weight.get_sorted_list(-1)
    print_top_key_range_items(top_ns_ip4, "top_ns_ip4")
    write_key_range_items(top_ns_ip4, temp_prefix + "top_ns_ip4.csv", "ipv4", "weight")
    top_ns_ip6 = ns_ip6_weight.get_sorted_list(-1)
    print_top_key_range_items(top_ns_ip6, "top_ns_ip6")
    write_key_range_items(top_ns_ip6, temp_prefix + "top_ns_ip6.csv", "ipv6", "weight")

    # produce M9
    save_m9(millions, ps, zp.dups, nd, asn_ag, asns, "20170228", fixed_weight, temp_prefix + "M9_bis.csv")

# actual main program, can be called by threads, etc.
if __name__ == '__main__':
    main()