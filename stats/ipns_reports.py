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
import ipaddress
import dnslook
import ip2as
import traceback
import sys

# the statistics of ASes look at the set of ASes used by a set of domain names.
# the names should be tabulated in "million ranges", from 0 to 5 (100, 1000, etc)
# or 6 (samples from COM domains). 
# the process is:
#     look at names in the domain list, in range "r"
#     for each name, look at the list of NS, or size N
#     for each NS, allocate a weight 1/N*A to the each AS listed in that NS

class group_report:
    def __init__(self, group_key):
        self.group_key = group_key
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

class group_list:
    def __init__(self):
        self.group_stat = dict()

    def add_group_set(self, group_set, mr):
        weight = 1.0/len(group_set)
        for group_key in group_set:
            if not group_key in self.group_stat:
                self.group_stat[group_key] = group_report(group_key)
            self.group_stat[group_key].add_weight(weight,mr)

    def set_fractions(self):
        sum_as = group_report(0)
        for group_key in self.group_stat:
            for i in range(0,6):
                sum_as.add_weight(self.group_stat[group_key].weight[i], i)
        for group_key in self.group_stat:
            self.group_stat[group_key].set_fraction(sum_as.weight)

    def top_list(self, n):
        self.set_fractions()
        s_list = sorted(self.group_stat.values(), key=lambda x: x.sort_weight, reverse=True)
        if len(s_list) > n:
            other = group_report(0)
            for group_data in s_list[n:]:
                for i in range(0,6):
                    other.fraction[i] += group_data.fraction[i]
                    other.weight[i] += group_data.weight[i]
            s_list = s_list[:n]
            s_list.append(other)
        return s_list

    def save_in_order(self, title, n, as_file):
        top_list = self.top_list(n)
        with open(as_file, "wt") as F:
            F.write(title + ",F100,F1000,F10K,F100K,F1M,FCom,S100,S1000,S10K,S100K,S1M,SCom\n")
            for group_data in top_list:
                F.write(str(group_data.group_key))
                for i in range(0,6):
                    F.write(","+str(group_data.fraction[i]))
                for i in range(0,6):
                    F.write(","+str(group_data.weight[i]))
                F.write("\n")

class as_list:
    def __init__(self):
        self.stats = group_list()
        self.nb_skipped = 0
        self.nb_bad_mr = 0
        
    def compute_as_set(nses, ns_list):
        as_set = set()
        for ns in nses:
            if ns in ns_list:
                for asn in ns_list[ns].ases:
                    as_key = "AS" + str(asn)
                    as_set.add(as_key)
        return as_set

    def nsas_stats(self, dns_list, ns_list):
        for d in dns_list:
            n = len(d.ns)
            mr = d.million_range
            try:
                as_set = as_list.compute_as_set(d.ns, ns_list)
                if len(as_set) == 0:
                    self.nb_skipped += 1
                    if self.nb_skipped <= 5:
                        print("Skip " + d.domain + ", " + str(len(d.ns)) + " NS, " + str(len(as_set)) + " AS.")
                elif mr < 0 or mr > 5:
                    self.bad_mr += 1
                    if self.bad_mr <= 5:
                        print("Skip " + d.domain + ", bad million range: " + str(mr))
                else:
                    self.stats.add_group_set(as_set, mr)
            except Exception as e:
                traceback.print_exc()
                print("Error processing NS for " + d.domain + " Range: " + str(mr) + "\nException: " + str(e))

    def save_in_order(self, n, as_file):
        self.stats.save_in_order("AS", n, as_file)

class net_list:
    def __init__(self):
        self.stats = group_list()
        self.nb_skipped = 0
        self.nb_bad_mr = 0

    def compute_net_set(nses, ns_list):
        net_set = set()
        for ns in nses:
            if ns in ns_list:
                for ip in ns_list[ns].ip:
                    try:
                        net = str(ipaddress.IPv4Network(ip + "/24", strict=False))
                        net_set.add(net)
                    except Exception as e:
                        print("Error processing IP for " + ns + ": " + str(ip) + "\nException: " + str(e))
                for ipv6 in ns_list[ns].ipv6:
                    try:
                        net = str(ipaddress.IPv6Network(ipv6 + "/48", strict=False))
                        net_set.add(net)
                    except Exception as e:
                        print("Error processing IPv6 for " + ns + ": " + str(ipv6) + "\nException: " + str(e))
        return net_set

    def nsnet_stats(self, dns_list, ns_list):
        for d in dns_list:
            n = len(d.ns)
            mr = d.million_range
            try:
                net_set = net_list.compute_net_set(d.ns, ns_list)
                if len(net_set) == 0:
                    self.nb_skipped += 1
                    if self.nb_skipped <= 5:
                        print("Skip " + d.domain + ", " + str(len(d.ns)) + " NS, no IP address")
                elif mr < 0 or mr > 5:
                    self.bad_mr += 1
                    if self.bad_mr <= 5:
                        print("Skip " + d.domain + ", bad million range: " + str(mr))
                else:
                    self.stats.add_group_set(net_set, mr)
            except Exception as e:
                traceback.print_exc()
                print("Error processing address for " + d.domain + " Range: " + str(mr) + "\nException: " + str(e))

    def save_in_order(self, n, net_file):
        self.stats.save_in_order("Network", n, net_file)

# Main entry point

if len(sys.argv) != 5:
    print("Usage: " + sys.argv[0] + " million_file ns_file as_report_file net_report_file")
    exit(-1)

million_file = sys.argv[1]
ns_file = sys.argv[2]
as_file = sys.argv[3]
net_file = sys.argv[4]

mf = dnslook.load_dns_file(million_file)
nt = dnslook.name_table()
nt.load(ns_file)
print("Loaded data for " + str(len(nt.table)) + " NS.")

al = as_list()
al.nsas_stats(mf, nt.table)

print("Found " + str(len(al.stats.group_stat)) + " ASes.")
if al.nb_skipped > 0:
    print("Skipped " + str(al.nb_skipped) + " domains with 0 AS data.")
if al.nb_bad_mr > 0:
    print("Skipped " + str(al.nb_bad_mr) + " domains with bad million rank.")
al.save_in_order(100,as_file)

nl = net_list()
nl.nsnet_stats(mf, nt.table)

print("Found " + str(len(nl.stats.group_stat)) + " Networks.")
if nl.nb_skipped > 0:
    print("Skipped " + str(nl.nb_skipped) + " domains with 0 addresses.")
if nl.nb_bad_mr > 0:
    print("Skipped " + str(nl.nb_bad_mr) + " domains with bad million rank.")
nl.save_in_order(1000,net_file)
