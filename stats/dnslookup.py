#!/usr/bin/python
# coding=utf-8
#
# This script "upgrades" result files collected with a previous version without documenting server
# and ASN. The program reads the old file, upgrade each name record, and copies the result to the
# new file.

import sys
import dns.resolver
import json
import traceback
import publicsuffix
import ip2as
import dnslook

if len(sys.argv) != 5:
    print("Usage: " + sys.argv[0] + " ip2as.csv publicsuffix.dat domain_list_file new_result_file")
    exit(1)

ip2as_file = sys.argv[1]
public_suffix_file = sys.argv[2]
domain_list_file = sys.argv[3]
new_result_file = sys.argv[4]

ps = publicsuffix.public_suffix()
if not ps.load_file(public_suffix_file):
    print("Could not load the suffixes")
    exit(1)

i2a = ip2as.ip2as_table()
if i2a.load(ip2as_file):
    print("Loaded i2a table of length: " + str(len(i2a.table)))
else:
    print("Could not load <" + ip2as_file + ">")

nb_lines = 0
nb_domains = 0
try:
    f_out = open(new_result_file, "wt")
    try: 
        for line in open(domain_list_file , "rt"):
            nb_lines += 1 
            d = dnslook.dnslook()
            if d.from_json(line):
                if len(d.ns) == 0:
                    print("Correcting: " + d.domain)
                    d.get_ns()
                    # if that was successful, try getting an IP address
                    if len(d.ns) > 0 and len(d.ip) == 0:
                        d.get_a()
                    # if that was successful, try filling other missing data
                    # if not, don't, because that would be a waste of time
                    if len(d.ip) > 0:
                        if len(d.ipv6) == 0:
                            d.get_aaaa()
                        if len(d.cname) == 0:
                            d.get_cname()
                if d.server == "":
                    d.get_server(ps)
                if d.as_number == 0:
                    d.get_asn(i2a)
                f_out.write(d.to_json() + "\n")
                nb_domains += 1
        print("All done")
    except Exception as e:
        traceback.print_exc()
        print("Cannot read file <" + domain_list_file  + ">\nException: " + str(e))
        print("Giving up");
    f_out.close()
except Exception as e:
    traceback.print_exc()
    print("Cannot create file <" + new_result_file  + ">\nException: " + str(e))
    print("Giving up");

print("nb_lines: " + str(nb_lines))
print("nb_domains: " + str(nb_domains))
