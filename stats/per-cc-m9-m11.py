
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
import functools

def extract_tld(dns_name):
    tld = ""
    name_parts = dns_name.split(".")
    while len(name_parts) > 0 and len(name_parts[-1]) == 0:
        name_parts = name_parts[:-1]
    if len(name_parts) > 0:
        tld = name_parts[-1]
    return tld

class m11_provider_stat:
    def __init__(self, ns_suffix):
        self.ns_suffix = ns_suffix
        self.sort_weight = 0
        self.count = []
        self.dnssec = []
        self.ratio = []
        for i in range(0,5):
            self.count.append(0.0)
            self.dnssec.append(0.0)
            self.ratio.append(0.0)

    def load(self, million_range, dns_sec, weight):
        if million_range >= 0 and million_range < 5:
            self.count[million_range] += weight
            if dns_sec > 0:
                self.dnssec[million_range] += weight

    def compute(self, r_count):
        category_weight = [ 0.1, 1, 1, 1, 1]
        self.sort_weight = 0
        for i in range(0,5):
            if self.count[i] == 0:
                self.ratio[i] = 0
            else:
                self.ratio[i] = self.dnssec[i]/self.count[i]
                if self.count[i] > 0:
                    self.sort_weight += category_weight[i]*self.count[i]/r_count[i]

class m11_providers_stat:
    def __init__(self):
        self.p_list = dict()
        self.r_count = []
        for i in range(0,5):
            self.r_count.append(0)

    def load(self, million_range, dns_sec, ns_suffix, nb_suffixes):
        weight = 1.0
        if nb_suffixes > 1:
            weight /= nb_suffixes
        if not ns_suffix in self.p_list:
            self.p_list[ns_suffix] = m11_provider_stat(ns_suffix)
        if million_range >= 0 and million_range < 5:
            self.r_count[million_range] += 1
        self.p_list[ns_suffix].load(million_range, dns_sec, weight)
            
    def top_list(self, n):
        for ns_suffix in self.p_list:
            self.p_list[ns_suffix].compute(self.r_count)
        s_list = sorted(self.p_list.values(), key=lambda x: x.sort_weight, reverse=True)
        if len(s_list) > n:
            other = m11_provider_stat("others")
            for s_data in s_list[n:]:
                for i in range(0,5):
                    other.count[i] += s_data.count[i]
                    other.dnssec[i] += s_data.dnssec[i]
            other.compute(self.r_count)
            s_list = s_list[:n]
            s_list.append(other)
        return s_list

    def save(self, n, title, metric_date, result_file):
        l =  self.top_list(n)
        with open(result_file,"wt") as F:
            s = "date," + title
            level_name = ["top 100", "101 to 1000", "1001 to 10000", "10001 to 100000", "100000 to 1M"]
            for i in range(0,5):
                s += "," + level_name[i]
            for i in range(0,5):
                s += ", C " + level_name[i]
            s += "\n"
            F.write(s)
            for v in l:
                s = metric_date + "," + v.ns_suffix
                for i in range(0,5):
                    s += "," + str(100.0*v.ratio[i])+"%"
                for i in range(0,5):
                    s += "," + str(v.count[i])
                s += "\n"
                F.write(s)

# Main
if len(sys.argv) != 7:
    print("Usage: " + sys.argv[0] + " stats_file public_suffix_file dups_file metric_date suffix_result_file cctld_result_file")
    exit(1)
stats_file = sys.argv[1]
ps_file = sys.argv[2]
dups_file = sys.argv[3]
metric_date = sys.argv[4]
suffix_result_file = sys.argv[5]
cctld_result_file = sys.argv[6]

ps = pubsuffix.public_suffix()
ps.load_file(ps_file)
print("found " + str(len(ps.table)) + " public suffixes.")
zp = zoneparser.zone_parser2(ps)
zp.load_dups(dups_file)
# Fill categories indices 0 to 4 with results from the million look ups
nb_fail = 0
suffix_list = m11_providers_stat()
cc_list = m11_providers_stat()

domainsFound = dict()
nb_domains_duplicate = 0
loaded = 0

# Compute the results per provider and per 
for line in open(stats_file, "rt"):
    dns_look = dnslook.dnslook()
    try:
        dns_look.from_json(line)
        loaded += 1
    except Exception as e:
        traceback.print_exc()
        print("Cannot parse <" + line  + ">\nException: " + str(e))
        continue
    if loaded%500 == 0:
        sys.stdout.write(".")
        sys.stdout.flush()
    # use suffixes in the million list!
    ns_this_name = dict()
    if dns_look.domain == "":
        continue
    if dns_look.domain in domainsFound:
        domainsFound[dns_look.domain] += 1
        nb_domains_duplicate += 1
        continue
    else:
        domainsFound[dns_look.domain] = 1
    million_range = dns_look.million_range
    dns_sec = 0
    if len(dns_look.ds_algo) > 0:
        dns_sec = 1
    ns_suffixes = set()
    if million_range >= 0 and million_range < 5 and len(dns_look.ns) > 0:
        for ns_name in dns_look.ns:
            ns_suffix = zoneparser.extract_server_suffix(ns_name, ps, zp.dups)
            if not ns_suffix in ns_suffixes:
                ns_suffixes.add(ns_suffix)
        nb_suffixes = len(ns_suffixes)
        for ns_suffix in ns_suffixes:
            suffix_list.load(million_range, dns_sec, ns_suffix, nb_suffixes)
    tld = extract_tld(dns_look.domain)
    #if len(tld) == 2:
    cc_list.load(million_range, dns_sec, tld, 1.0)
print("\nFound " + str(len(domainsFound)) + " domains, " + str(nb_domains_duplicate) + " duplicates.")
# save the CSV files 
suffix_list.save(20, "dns provider", metric_date, suffix_result_file)
cc_list.save(len(cc_list.p_list), "cc", metric_date, cctld_result_file)


