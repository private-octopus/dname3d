
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

if len(sys.argv) != 3:
    print("Usage: " + sys.argv[0] + " stats_file cleaned_file")
    exit(1)
stats_file = sys.argv[1]
cleaned_file = sys.argv[2]

domainsFound = dict()
nb_domains_duplicate = 0

with open(cleaned_file, "wt") as F:
    loaded = 0
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
        F.write(line)

print("\nFound " + str(loaded) + " lines, " + str(len(domainsFound)) + " domains, " + str(nb_domains_duplicate) + " duplicates.")