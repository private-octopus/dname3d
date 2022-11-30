#!/usr/bin/python
# coding=utf-8
# 
# Manage a list of servers, and obtain their addresses.
# This is used for example to draw statistics on name
# servers. Each name server object includes:
# - Name
# - Date IPv4 queried
# - Date IPv6 queried
# - List of IPv4 addresses (text representation)
# - List of IPv6 addresses (text representation)
# - List of network prefixes found in the IP addresses
#
# The names are loaded either from a text file or through an API.
# The loading process ensures that a given name occurs only once.
#
# In production, a script runs every day, with a new file
# created each month. The script runs after `do_dnslookup.py`,
# which every day obtains data for a new set of samples from
# the million list. This sampling may find new server names,
# not presnet in the current file. The order of operation
# will be:
# 
# - if in a new month, create a new file for that month.
# - load the current server list, as computed yesterday,
# - load the last "dns million" sample list.
# - extract server names from the dns million list,
#   and add these names to the current server list
# - find the names for which the IPv4 or IPv6 addresses
#   are not known.
# - perform DNS queries and store results in the corresponding
#   server name object.
# - for all names in the sample list, create an new
#   name + prefix record, with name + set of prefixes found.
#
# On a multicore machine, we will have to option of using multiple
# processes to perform DNS queries in parallel. This require a
# parallel version of the steps listed above:
#
# - get the list of necessary queries
# - split that list into one sublist per process
# - run the queries in separate processes
# - save queries results at the end of each process
# - read these results in the main process and add them to
#   the object in the server list.

from distutils.errors import UnknownFileError
import sys
import dns.resolver
import ipaddress
import json
import traceback
import dnslook
import time

def to_json_addresses(x):
        jsa = "["
        is_first = True
        for item in x:
            if not is_first:
                jsa += ","
            is_first = False
            jsa += "\"" + item + "\""
        jsa += "]"
        return(jsa)

class name_entry:
    def __init__(self):
        self.domain = ""
        self.ip = []
        self.ipv6 = []
        self.prefixes = set()
        self.ip_queried = False
        self.ipv6_queried = False

    def to_json(self):
        js = "{\"domain\":\"" + self.domain + "\""
        if self.ip_queried:
            js += ",\"ip\":" + to_json_addresses(self.ip)
        if self.ipv6_queried:      
            js += ",\"ipv6\":" + to_json_addresses(self.ipv6)
        if len(self.prefixes) > 0:
            js += ",\"prefixes\":" + to_json_addresses(self.prefixes)
        js += "}"
        return(js)
    
    def from_json(self, js):
        ret = True
        try:
            jd = json.loads(js)
            if not 'domain' in jd:
                print("No domain in " + js)
                ret = False
            else:
                self.domain = jd['domain']
                if 'ip' in jd:
                    self.ip_queried = True
                    self.ip = jd['ip']
                if 'ipv6' in jd:
                    self.ipv6_queried = True
                    self.ipv6 = jd['ipv6']
                if 'prefixes' in jd:
                    self.prefixes = jd['prefixes']
        except Exception as e:
            traceback.print_exc()
            print("Cannot parse <" + js + ">")
            print("error: " + str(e))
            ret = False
        return(ret)

    def add_prefix(self, text_address, length):
        network = ipaddress.ip_network(text_address + "/" + str(length), strict=False)
        prefix = str(network.network_address) + "/" + str(length)
        if not prefix in self.prefixes:
            self.prefixes.add(prefix)

    def get_a(self, resolver):
        self.ip = []
        try:
            addresses = resolver.query(self.domain, 'A')
            for ipval in addresses:
                ip_text = ipval.to_text()
                self.ip.append(ip_text)
                self.add_prefix(ip_text, 24)
            self.ip_queried = True
        except Exception as e:
            pass

    def get_aaaa(self, resolver):
        self.ipv6 = []
        try:
            addresses = resolver.query(self.domain, 'AAAA')
            for ipval in addresses:
                ipv6_text = ipval.to_text()
                self.ipv6.append(ipv6_text)
                self.add_prefix(ipv6_text, 48)
            self.ipv6_queried = True
        except Exception as e:
            pass

class name_entry_list:
    def __init__(self):
        self.resolver=dns.resolver.Resolver()
        self.names = dict()
        self.bucket_id = 0
        self.target_file = ""
        self.is_complete = False

    def add_name(self, name):
        if not name in self.names:
            one_entry = name_entry()
            one_entry.domain = name
            self.names[name] = one_entry

    def add_json(self, line):
        ret = True
        one_entry = name_entry()
        if one_entry.from_json(line):
            self.names[one_entry.domain] = one_entry
        else:
            ret = False
        return ret

    def load(self, file_name):
        i_line = 0
        print("loading: " + file_name)
        for line in open(file_name, "rt"):
            i_line += 1
            if not self.add_json(line):
                print("Error on " + file_name + " line " + str(i_line) + ":<" + line.strip() + ">")
                return False
        return True

    def save(self, file_name):
        with open(file_name, "wt") as F:
            for name in self.names:
                line = self.names[name].to_json() + "\n"
                F.write(line)

    def solve(self):
        for name in self.names:
            if not self.names[name].ip_queried:
                self.names[name].get_a(self.resolver)
            if not self.names[name].ipv6_queried:
                self.names[name].get_aaaa(self.resolver)

    def prepare_bucket(self, nb_buckets, ns_cache_bucket_prefix):
        buckets = []
        for i in range(0,nb_buckets):
            buckets.append(name_entry_list())
            buckets[i].bucket_id = i
            buckets[i].target_file = ns_cache_bucket_prefix + "_" + str(i) + ".json"
        bucket_id = 0
        buckets_full = False
        for name in self.names:
            one_entry = self.names[name]
            if not one_entry.ip_queried or not one_entry.ipv6_queried:
                buckets[bucket_id].names[one_entry.domain] = one_entry
                bucket_id += 1
                if bucket_id >= nb_buckets:
                    buckets_full = True
                    bucket_id = 0
        
        if not buckets_full:
            while len(buckets) > 0 and len(buckets[-1].names) == 0:
                buckets = buckets[0:-1]

        return buckets

    def set_prefix_list(self):
        for name in self.names:
            for prefix in self.names[name].prefixes:
                self.prefixes.add_prefix(prefix)


