#!/usr/bin/python
# coding=utf-8
#
# Parse the zone file, retrieve the NS records, and maybe the corresponding A/AAAA records
# The goal is to count the total number of name servers, and also to retrieve the top
# 10,000 name servers, so we can do statistics on them. 
#
# We can think of two modes of operations:
# - just count all names

import traceback
import hyperloglog
import gzip
import traceback
import functools
import sys
import ipaddress
import pubsuffix

class lru_list_entry:
    def __init__(self, target_class):
        self.lru_next = ""
        self.lru_previous = ""
        self.data = target_class()

class lru_list:
    def __init__(self, target_number, target_class):
        self.lru_first = ""
        self.lru_last = ""
        self.target_number = target_number
        self.target_class = target_class
        self.table = dict()

    def add(self, key):
        ret = True
        try:
            # first manage the lru list
            if key in self.table:
                # bring to top
                if not key == self.lru_first:
                    if self.table[key].lru_previous == "":
                        print("Could not promote <" + key + "> (" + self.table[key].lru_previous + "," + self.table[key].lru_next + ") after " + str(len(self.table)) + " (" + self.lru_first + "," + self.lru_last + ")")
                        ret = False
                    try:
                        if key == self.lru_last:
                            self.lru_last = self.table[key].lru_previous
                            self.table[self.lru_last].lru_next = ""
                        else:
                            self.table[self.table[key].lru_next].lru_previous = self.table[key].lru_previous
                            self.table[self.table[key].lru_previous].lru_next = self.table[key].lru_next
                        self.table[key].lru_previous = ""
                        self.table[key].lru_next = self.lru_first
                        self.table[self.lru_first].lru_previous = key
                        self.lru_first = key
                    except:
                        traceback.print_exc()
                        print("Could not promote <" + key + "> (" + self.table[key].lru_previous + "," + self.table[key].lru_next + ") after " + str(len(self.table)) + " (" + self.lru_first + "," + self.lru_last + ")")
                        ret = False
            else:
                # add an entry to the list
                self.table[key] = lru_list_entry(self.target_class)
                if len(self.table) > self.target_number:
                    #if the list is full, pop the least recently used entry
                    old = self.lru_last
                    self.lru_last = self.table[old].lru_previous
                    self.table[self.lru_last].lru_next = ""
                    self.table.pop(old)
                if self.lru_first == "":
                    self.lru_first = key
                    self.lru_last = key
                else:
                    self.table[key].lru_next = self.lru_first
                    self.table[self.lru_first].lru_previous = key
                    self.lru_first = key
        except:
            traceback.print_exc()
            print("Could not add <" + key + "> (" + self.lru_first + "," + self.lru_last + ") after " + str(len(self.table)))
            ret = False
        return ret


class ns_list_entry:
    def __init__(self):
        self.hit_count = 0
        self.approx_names = hyperloglog.hyperloglog(4)

class zone_parser:
    def __init__(self, ps, limit):
        self.sf_list = lru_list(limit, ns_list_entry)
        self.ns_list = lru_list(limit, ns_list_entry)
        self.sf_list = lru_list(limit, ns_list_entry)
        self.hit_count = 0
        self.approx_names = hyperloglog.hyperloglog(4)
        self.approx_servers = hyperloglog.hyperloglog(4)
        self.approx_services = hyperloglog.hyperloglog(4)
        self.ps = ps

    def add(self, ns_name, fqdn):
        ret = self.ns_list.add(ns_name)
        if ret:
            # update the counters for the entry
            self.ns_list.table[ns_name].data.hit_count += 1
            self.ns_list.table[ns_name].data.approx_names.add(fqdn)
            # account for the total number of domains and servers
            self.hit_count += 1
            self.approx_names.add(fqdn)
            self.approx_servers.add(ns_name)
            # extract the public suffix
            x,is_suffix = self.ps.suffix(ns_name)
            if x == "" or not is_suffix:
                np = ns_name.split(".")
                l = len(np)
                while l >= 2 and len(np[l-1]) == 0:
                    l -= 1
                if l > 2:
                    x = (np[l-2] + "." + np[l-1])
            if x == "":
                print("Cannot add empty suffix for " + ns_name + " (" + str(is_suffix) + ")")
            else:
                # special rule for AWS DNS
                if x.startswith("awsdns-"):
                    p = x.split(".")
                    x = "awsdns-??"
                    for np in p[1:]:
                        x += "." + np
                self.approx_services.add(x)
                ret = self.sf_list.add(x)
                if ret:
                    self.sf_list.table[x].data.hit_count += 1
                    self.sf_list.table[x].data.approx_names.add(fqdn)

        return ret

    def add_zone_file(self, file_name):
        for line in open(file_name , "rt", encoding="utf-8"):
            # parse the input line
            parts = line.split("\t")
            # if this is a "NS" record, submit.
            if len(parts) == 5 and parts[2] == "in" and parts[3] == "ns":
                ns_name = parts[4].strip()
                if ns_name == "":
                    print("Cannot add empty ns name from: <" + line.strip() + ">")
                elif not self.add(ns_name, parts[0]):
                    print("Error parsing " + line.strip())
                    break

    def save(self, file_name):
        f = open(file_name , "wt", encoding="utf-8")
        f.write("table,server,nb_hits,est_names," + self.approx_names.header_full_text("h") + "\n");
        x = self.approx_names.evaluate()
        f.write("top,names," + str(self.hit_count) + "," + str(x) + "," + self.approx_names.to_full_text() + "\n")
        x = self.approx_services.evaluate()
        f.write("top,services," + str(self.hit_count) + "," + str(x) + "," + self.approx_services.to_full_text() + "\n")
        x = self.approx_servers.evaluate()
        f.write("top,servers," + str(self.hit_count) + "," + str(x) + "," + self.approx_servers.to_full_text() + "\n")
        # TODO: sort by highest value before printing.
        sf = self.sf_list.lru_first
        while sf != "":
            # save each top entry
            y = self.sf_list.table[ns].data.approx_names.evaluate()
            f.write("sf," + sf + "," + str(self.sf_list.table[sf].data.hit_count) + "," + str(y) + "," + self.sf_list.table[sf].data.approx_names.to_full_text() + "\n")
            sf = self.sf_list.table[sf].lru_next
        ns = self.ns_list.lru_first
        for ns in self.ns_list.table:
            # save each top entry
            y = self.ns_list.table[ns].data.approx_names.evaluate()
            f.write("ns," + ns + "," + str(self.ns_list.table[ns].data.hit_count) + "," + str(y) + "," + self.ns_list.table[ns].data.approx_names.to_full_text() + "\n")
            ns = self.ns_list.table[ns].lru_next
        f.close()





