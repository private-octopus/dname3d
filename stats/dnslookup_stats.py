#!/usr/bin/python
# coding=utf-8
#
# This script extracts the metric M9 from the statistics collected on the
# selected million names, and from the statistics on the COM zone.
# 
# The M9 metrict has the following data:
#
# M9.1.* -- statistics for top 100 hosts
# M9.2.* -- statistics for next 900 hosts
# M9.3.* -- statistics for next 9000 hosts
# M9.4.* -- statistics for next 90000 hosts
# M9.5.* -- statistics for next 900000 hosts
# M9.5.* -- statistics for .COM hosts
#
# For each class, we measure:
#
# M9.*.1, service name -- share of names served by a given server with at least 1% market share
# M9.*.2 -- number of services for 50% of names
# M9.*.3 -- number of services for 90% of names
# M9.*.4 -- average number of services per name
#
# Example:
# M9.2.1,2017-01-31,v2.0, cloudflare.com , 0.209763
# M9.2.1,2017-01-31,v2.0, AWS , 0.11023
# M9.2.1,2017-01-31,v2.0, etc. , 0.05000
# M9.2.2,2017-01-31,v2.0, , 9
# M9.2.3,2017-01-31,v2.0, , 120
# M9.2.4,2017-01-31,v2.0, , 1.2

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
        try:
            dns_look.from_json(line)
            stats.append(dns_look)
            loaded += 1
        except Exception as e:
            traceback.print_exc()
            print("Cannot parse <" + line  + ">\nException: " + str(e))
        if loaded%500 == 0:
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
        self.nb_names = 0
        self.total = 0
        self.service_list = dict()
        self.full_list = []

    def add(self, name, count):
        self.total += count
        if name in self.service_list:
            list_index = self.service_list[name]
            self.full_list[list_index].count += count
        else:
            list_index = len(self.full_list)
            self.service_list[name] = list_index
            self.full_list.append(stats_one_entry(name, count))

    def add_name_count(self, nb_names):
        self.nb_names += nb_names

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

    def m9(self, rank, m9date, F):
        metric = "M9." + str(rank+1); 
        if self.nb_names > 0:
            F.write(metric + ".1," + m9date +",v2.0, ," + str(self.total/self.nb_names) + "\n")
        F.write(metric + ".2," + m9date + ",v2.0, ," + str(self.p50) + "\n")
        F.write(metric + ".3," + m9date + ",v2.0, ," + str(self.p90) + "\n")
        for i in range(0,20):
            fraction = self.full_list[i].count/self.total
            if i > 4 and fraction < 0.005:
                break
            F.write(metric + ".4," + m9date + ",v2.0, " + self.full_list[i].name + "," + str(fraction) + "\n")
           


# Main

million_file = sys.argv[2]
public_suffix_file = sys.argv[3]
dups_file = sys.argv[4]
zone_result_file = sys.argv[5]
result_file = sys.argv[6]
m9_date = sys.argv[7]


print("Set to compute metric M9 in " + result_file + " for " + m9_date)

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

# Process the statistics acquired from the million list.
# Compute a list of per service statistics (ns_this_name),
# each containing a vector of 6 numbers: 0..5 are the sum
# of name of each category from the million file, [5] is
# the sum of names for this service in the com file.

# First prepare an empty table of 6 categories.
level_name = ["top 100", "next 900", "next 9000", "next 90000", "next 900000", "COM" ]
level_stats = []
for i in range(0,6):
    level_stats.append(stats_one())

# Fill the category index 5 with results from the com zone
for service in zp.sf_dict:
    level_stats[5].add(service, zp.sf_dict[service].name_count)
level_stats[5].add_name_count(zp.name_count)

# Fill categories indices 0 to 4 with results from the million look ups
nb_fail = 0
for dns_lookup in stats:
    # use suffixes in the million list!
    ns_this_name = dict()
    if dns_lookup.domain == "":
        continue
    m_class = get_million_class(dns_lookup.domain, zp.millions)
    if m_class > 4:
        print("could not find <" + dns_lookup.domain + "> in millions.")
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
        level_stats[m_class].add(ns_suffix, 1)
    level_stats[m_class].add_name_count(1)

# Compute the statistics for each category
for m_class in range(0,6):
    level_stats[m_class].compute()
    level_stats[m_class].comment(level_name[m_class])

# publish the metric file
print("saving metric M9 in " + result_file + " for " + m9_date)
with open(result_file,"wt") as F:
    for m_class in range(0,6):
        level_stats[m_class].m9(m_class, m9_date, F)

