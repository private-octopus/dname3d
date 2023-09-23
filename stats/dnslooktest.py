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

if len(sys.argv) < 5:
    print("Usage: " + sys.argv[0] + "ip2as.csv ip2as6.csv pub_suffixes domain-name*")
    exit(1)

ip2as_file = sys.argv[1]
ip2as6_file = sys.argv[2]
public_suffix_file = sys.argv[3]
domains = sys.argv[4:]

i2a = ip2as.load_ip2as(ip2as_file)
i2a6 = ip2as.load_ip2as(ip2as6_file)

ps = pubsuffix.public_suffix()

if not ps.load_file(public_suffix_file):
    print("Could not load the suffixes from" + public_suffix_file )

stats = []
for x in range(0,7):
    stats.append(0)

v = dnslook.dnslook()

for domain in domains:
    v.get_domain_data(domain, ps, i2a, i2a6, stats, rank=7)
    js = v.to_json()
    print(js)
    for ipv4 in v.ip:
        print(ipv4)
        as_number = i2a.get_as_number(ipv4)
        print(ipv4 + " -> AS" + str(as_number))
    for ipv6 in v.ipv6:
        as_number = i2a6.get_as_number(ipv6)
        print(ipv6 + " -> AS" + str(as_number))
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