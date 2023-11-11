#!/usr/bin/python
# coding=utf-8
#
# The ns_dict file collects the list of NS server names referenced
# in the "millions" file, and store IP addresses and AS numbers for these servers.

import sys
import dns.resolver
import json
import traceback
import pubsuffix
import ip2as
import dnslook
import random
import million_random
import dns_bucket
import zoneparser
import time
import concurrent.futures
import os

class ns_dict:
    def __init__(self):
        self.d = dict()
        self.nb_duplicate = 0

    def load_ns_file(self, ns_json, dot_after=0):
        loaded = 0
        for line in open(ns_json, "rt"):
            loaded += 1
            ns_item = dnslook.dnslook()
            try:
                ns_item.from_json(line)
                if ns_item.domain in self.d:
                    self.nb_duplicate += 1
                else:
                    self.d[ns_item.domain] = ns_item
            except Exception as e:
                traceback.print_exc()
                print("Cannot parse <" + line  + ">\nException: " + str(e))
            if dot_after > 0 and loaded%dot_after == 0:
                sys.stdout.write(".")
                sys.stdout.flush()
        if dot_after > 0:
            print("")
        print("Loaded " + str(len(self.d)) + " ns items out of " + str(loaded) + ", " + str(self.nb_duplicate) + " duplicates")
        return

    def save_ns_file(self, ns_json, dot_after=0):
        saved = 0
        with open(ns_json, "wt") as F:
            for ns in self.d:
                ns_item = self.d[ns]
                if ns_item.nb_queries > 0:
                    js = ns_item.to_json()
                    F.write(js + "\n")
                    saved += 1
                    if dot_after > 0 and saved%dot_after == 0:
                        sys.stdout.write(".")
                        sys.stdout.flush()
        if dot_after > 0:
            print("")
        print("Saved " + str(saved) + " ns items out of " + str(len(self.d)))
        return

    def add_ns_name(self, domain):
        if not domain in self.d:
            ns_item = dnslook.dnslook()
            ns_item.domain = domain
            self.d[domain] = ns_item

    def random_list(self, n, only_news=False):
        sd = dict()
        nb = 0
        for ns in self.d:
            ns_item = self.d[ns]
            if not only_news or (ns_item.nb_queries == 0) or (ns_item.dns_timeout > 0 and ns_item.nb_queries < 3):
                nb += 1
                if nb <= n:
                    sd[nb] = ns_item
                else:
                    r = random.randint(1,nb)
                    if r == nb:
                        x = random.randint(0,n-1)
                        sd[x] = ns_item
        targets = []
        for x in sd:
            targets.append(sd[x])
        return targets

    def get_data(self, targets, ps, i2a, i2a6, stats):
        for target in targets:
            ns_item = self.d[target.domain]
            if ns_item.nb_queries == 0:
                ns_item.get_domain_data(target.domain, ps, i2a, i2a6, stats)
            else:
                ns_item.retry_domain_data(ps, i2a, i2a6, stats)

