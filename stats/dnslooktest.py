#!/usr/bin/python
# coding=utf-8
#
# The module dnslookup assesses properties of a domain name: find its root zone, find who manages it,
# find whether the name is an alias, resolve the chain of aliases, find who serves the IP address.
# The goal is to observe concentration in both the name services and the distribution services.
#
# For name services, we capture the list of NS servers for the specified name.
# For distribution services, we capture the list of CNames and the IP addresses.
# We format the results as a JSON entry.
#
# The procedures are embedded in the class "dnslook", with three main methods:
#
# get_domain_data(self, domain): read from the DNS the data for the specified domain, and 
# store the results in the referenced object.
# 
# to_json(self): serialize the object as a JSON string.
#
# from_json(self, js): load the object value from a JSON string. 

import sys
import dns.resolver
import json
import traceback
import dnslook
import ip2as
import pubsuffix

# Basic tests

if len(sys.argv) < 4:
    print("Usage: " + sys.argv[0] + "ip2as.csv pub_suffixes domain-name*")
    exit(1)

ip2as_file = sys.argv[1]
public_suffix_file = sys.argv[2]
domains = sys.argv[3:]

i2a = ip2as.ip2as_table()

if i2a.load(ip2as_file):
    print("Loaded ip2as table of length: " + str(len(i2a.table)))
else:
    print("Could not load \"" + ip2as_file + "\"")

ps = pubsuffix.public_suffix()

if not ps.load_file(public_suffix_file):
    print("Could not load the suffixes from" + public_suffix_file )

stats = []
    for x in range(0,7):
        stats.append(0)

v = dnslook.dnslook()

for domain in domains:
    v.get_domain_data(domain, ps, i2a, stats)
    js = v.to_json()
    print(js)
    w = dnslook.dnslook()
    if w.from_json(js):
        js2 = w.to_json()
        if js2 == js:
            print("Parsed json matches input")
        else:
            print("Converted from json differs:")
            print(js2)
    else:
        print("Cannot parse json output")