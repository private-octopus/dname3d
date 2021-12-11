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
import functools

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

def get_prefix(name,ps):
    x,is_suffix = ps.suffix(name)
    if x == "" or not is_suffix:
        np = name.split(".")
        l = len(np)
        while l >= 2 and len(np[l-1]) == 0:
            l -= 1
        if l >= 2:
            x = (np[l-2] + "." + np[l-1])
        if x == "":
            print("Escaped " + name + " to " + x)
    return x

def get_million_class(fqdn, millions):
    y = fqdn
    million_rank = 5
    if y.endswith("."):
        y = y[0:-1]
    if y in millions:
        million_rank = millions[y]
    else:
        x = get_prefix(y,ps)
        if x in millions:
            million_rank = millions[x]
        else:
            print("for " + y + " got " + x + ", not in millions")
    return million_rank

class rank_count:
    def __init__(self):
        self.count = [0,0,0,0,0,0]

class stats_one_entry:
    def __init__(self, name, count):
        self.name = name
        self.count = count

def compare_stats_entry(item, other):
    if item.count < other.count:
        return -1
    elif item.count > other.count:
        return 1
    elif item.name < other.name:
        return -1
    elif item.name > other.name:
        return 1
    else:
        return 0

class stats_one:
    def __init__(self):
        self.p50 = 0
        self.p90 = 0
        self.total = 0
        self.full_list = []

    def add(self, name, count):
        self.total += count
        self.full_list.append(stats_one_entry(name, count))

    def compute(self):
        self.full_list.sort(key=functools.cmp_to_key(compare_stats_entry), reverse=True)
        cumul50 = 50*self.total/100
        cumul90 = 90*self.total/100
        cumul = 0.0
        rank = 0
        for entry in self.full_list:
            rank += 1
            cumul += entry.count
            if self.p50 == 0 and cumul >= cumul50:
                self.p50 = rank
            if cumul >= cumul90:
                self.p90 = rank
                break
    def comment(self, name):
        print(name+":" + str(len(self.full_list)) + ", 50%: " + str(self.p50) + ", 90%: " + str(self.p90))
        for i in range(0,5):
            print(name + "["+ str(i) + "]: " + self.full_list[i].name + ", " + str(self.full_list[i].count/self.total))


# Main

million_file = sys.argv[2]
public_suffix_file = sys.argv[3]
dups_file = sys.argv[4]
zone_result_file = sys.argv[5]
result_file = sys.argv[6]

ps = pubsuffix.public_suffix()
if not ps.load_file(public_suffix_file):
    print("Could not load the suffixes")
    exit(1)
   
print("\nLoaded " + str(len(ps.table)) + " suffixes.")

zp = zoneparser.zone_parser2(ps)
zp.load_million(million_file)
print("\nLoaded " + str(len(zp.millions)) + " millions.")
zp.load_dups(dups_file)
print("loaded the dependencies")

zp.load_partial_result(zone_result_file)
print("Loaded " + str(len(zp.sf_dict)) + " services from " + zone_result_file + ".")

test_rank = rank_count()
for name in zp.millions:
    test_rank.count[zp.millions[name]] += 1
rank_string = ""
for count in test_rank.count:
    rank_string += str(count) + ","
print("test ranks:" + rank_string)

stats = load_dns_file(sys.argv[1])
print("\nLoaded " + str(len(stats)) + " lines.")

ns_dict = dict()

nb_fail = 0
total_rank = rank_count();
for dns_lookup in stats:
    # check that the names used in the domain property are consistent with the names in the million list.
    # may want to use suffixes in the million list!
    ns_this_name = dict()
    m_class = get_million_class(dns_lookup.domain, zp.millions)
    total_rank.count[m_class] += 1
    if m_class > 4:
        print("could not find " + dns_lookup.domain + " in millions.")
        nb_fail += 1
        if nb_fail > 10:
            break
    for ns_name in dns_lookup.ns:
        ns_suffix = zoneparser.extract_server_suffix(ns_name, ps, zp.dups);
        if ns_suffix in ns_this_name:
            ns_this_name[ns_suffix] += 1
        else:
            ns_this_name[ns_suffix] = 1
    for ns_suffix in ns_this_name:
        if not ns_suffix in ns_dict:
            ns_dict[ns_suffix] = rank_count()
        ns_dict[ns_suffix].count[m_class] += 1
print("processed the names")


with open(result_file,"wt") as f:
    f.write("name, 100, 1K, 10K, 100K, 1M, all\n");
    f.write("count");
    for count in total_rank.count:
        f.write("," + str(count));
    f.write("\n")
    for ns_name in ns_dict:
        f.write(ns_name)
        m_class = ns_dict[ns_name]
        for m_count in m_class.count:
            f.write(","+str(m_count))
        f.write("\n")
        
print("saved " + str(len(ns_dict)) + " services.")

level_stats = []
level_name = "top_10"
for i in range(0,5):
    level_name += "0"
    level_stats.append(stats_one())
    for name in ns_dict:
        if ns_dict[name].count[i] > 0:
            level_stats[i].add(name, ns_dict[name].count[i])
    level_stats[i].compute()
    level_stats[i].comment(level_name)

zone_stats = stats_one()
for service in zp.sf_dict:
    zone_stats.add(service, zp.sf_dict[service].hit_count)
zone_stats.compute()
zone_stats.comment("Com")
