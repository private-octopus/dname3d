#!/usr/bin/python
# coding=utf-8
#
# The module nslookup builds a table of name servers for which
# there are references in the million file names or in other
# samples. For each reference name, it builds:
#
# - a list of IP and IPv6 addresses for that name
# - a list of ASes serving these IP or IPv6 addresses
#
import dnslook
import ip2as
import traceback

class name_table:
    def __init__(self):
        self.table = dict()

    def add_name(self, domain, i2a):
        if not domain in self.table:
            try:
                d = dnslook.dnslook()
                d.domain = domain
                d.get_a()
                d.get_aaaa()
                d.get_asn(i2a)
                self.table[domain] = d
            except Exception as e:
                traceback.print_exc()
                print("Cannot find addresses of <" + domain  + ">\nException: " + str(e))

    def load(self, file_name, add=True):
        added = dnslook.load_dns_file(file_name)
        if add:
            for d in added:
                if not d.domain in self.table:
                    self.table[d.domain] = d
        else:
            self.table=added

    def save(self,file_name):
        with open(file_name, "wt") as f_out:
            for domain in self.table:
                try:
                    f_out.write(self.table[domain].to_json() + "\n")
                except Exception as e:
                    traceback.print_exc()
                    print("Cannot save addressed for domain <" + target.domain  + ">\nException: " + str(e))
                    break

    # we expect the addition of new names to run in three steps:
    # - first, get a list of the "dnslook" objects that have not been found
    # - split the list and run separate buckets to search the ns names
    # - then load each of the produced lists back in the table
    #
    def schedule_ns(self, dns_list):
        scheduled = set()
        for d in dns_list:
            for ns in d.ns:
                if not ns in self.table and not ns in scheduled:
                    scheduled.add(ns)
        return list(scheduled)

# the statistics of ASes look at the set of ASes used by a set of domain names.
# the names should be tabulated in "million ranges", from 0 to 5 (100, 1000, etc)
# or 6 (samples from COM domains). 
# the process is:
#     look at names in the domain list, in range "r"
#     for each name, look at the list of NS, or size N
#     for each NS, allocate a weight 1/N*A to the each AS listed in that NS

class as_report:
    def __init__(self, asn):
        self.asn = asn
        self.weight = [ 0, 0, 0, 0, 0, 0]
        self.fraction = [ 0, 0, 0, 0, 0, 0]
        self.sort_weight = 0

    def add_weight(self, weight, mr):
        self.weight[mr] += weight

    def set_fraction(self, total):
        for i in range(0,6):
            if total[i] > 0:
                self.fraction[i] = self.weight[i] / total[i]
        self.sort_weight = self.fraction[0]/10.0
        for i in range(1,6):
            self.sort_weight += self.fraction[i]

class as_list:
    def __init__(self):
        self.as_stat = dict()

    def compute_as_set(nses, ns_list):
        as_set = set()
        for ns in nses:
            if ns in ns_list:
                for asn in ns_list[ns].ases:
                    as_key = "AS" + str(asn)
                    as_set.add(as_key)
        return as_set

    def add_as_set(self, as_set, mr):
        weight = 1.0/len(as_set)
        for as_key in as_set:
            if not as_key in self.as_stat:
                self.as_stat[as_key] = as_report(as_key)
            self.as_stat[as_key].add_weight(weight,mr)

    def nsas_stats(self, dns_list, ns_list):
        for d in dns_list:
            n = len(d.ns)
            mr = d.million_range
            try:
                as_set = as_list.compute_as_set(d.ns, ns_list)
                if len(as_set) == 0:
                    print("Skip " + d.domain + ", " + str(len(d.ns)) + " NS, " + str(len(as_set)) + " AS.")
                elif mr < 0 or mr > 5:
                    print("Skip " + d.domain + ", bad million range: " + str(mr))
                else:
                    self.add_as_set(as_set, mr)
            except Exception as e:
                traceback.print_exc()
                print("Error processing " + d.domain + " Range: " + str(mr) + "\nException: " + str(e))

    def set_fractions(self):
        sum_as = as_report(0)
        for asn in self.as_stat:
            for i in range(0,6):
                sum_as.add_weight(self.as_stat[asn].weight[i], i)
        for asn in self.as_stat:
            self.as_stat[asn].set_fraction(sum_as.weight)

    def top_list(self, n):
        self.set_fractions()
        s_list = sorted(self.as_stat.values(), key=lambda x: x.sort_weight, reverse=True)
        if len(s_list) > n:
            other = as_report(0)
            for as_data in s_list[n:]:
                for i in range(0,6):
                    other.fraction[i] += as_data.fraction[i]
                    other.weight[i] += as_data.weight[i]
            s_list = s_list[:n]
            s_list.append(other)
        return s_list

    def save_in_order(self, n, as_file):
        top_list = self.top_list(n)
        with open(as_file, "wt") as F:
            F.write("AS,F100,F1000,F10K,F100K,F1M,FCom,S100,S1000,S10K,S100K,S1M,SCom\n")
            for as_data in top_list:
                F.write(str(as_data.asn))
                for i in range(0,6):
                    F.write(","+str(as_data.fraction[i]))
                for i in range(0,6):
                    F.write(","+str(as_data.weight[i]))
                F.write("\n")


# TODO: remove that

import sys

million_file = sys.argv[1]
ip2as_file = sys.argv[2]
result_file = sys.argv[3]
as_file = sys.argv[4]

mf = dnslook.load_dns_file(million_file)
i2a = ip2as.ip2as_table()
if i2a.load(ip2as_file):
    print("Loaded i2a table of length: " + str(len(i2a.table)))
else:
    print("Could not load <" + ip2as_file + ">")
nt = name_table()

nt.load(result_file)
print("Loaded data for " + str(len(nt.table)) + " NS.")

out_list = nt.schedule_ns(mf)
print("Found " + str(len(out_list)) + " names")

n=0
for name in out_list:
    nt.add_name(name, i2a)
    n += 1
    if n >= 100:
        break

print("Processed " + str(len(nt.table)) + " names")

nt.save(result_file)

al = as_list()
al.nsas_stats(mf, nt.table)

print("Found " + str(len(al.as_stat)) + " ASes.")
al.save_in_order(100,as_file)
