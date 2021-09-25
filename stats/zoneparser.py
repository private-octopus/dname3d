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
import traceback

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
            y = self.sf_list.table[sf].data.approx_names.evaluate()
            f.write("sf," + sf + "," + str(self.sf_list.table[sf].data.hit_count) + "," + str(y) + "," + self.sf_list.table[sf].data.approx_names.to_full_text() + "\n")
            sf = self.sf_list.table[sf].lru_next
        ns = self.ns_list.lru_first
        while ns != "":
            # save each top entry
            y = self.ns_list.table[ns].data.approx_names.evaluate()
            f.write("ns," + ns + "," + str(self.ns_list.table[ns].data.hit_count) + "," + str(y) + "," + self.ns_list.table[ns].data.approx_names.to_full_text() + "\n")
            ns = self.ns_list.table[ns].lru_next
        f.close()

# version 2 of the zone parser does not rely on LRU,
# but only stores the "service names". Running version 1 on
# the COM zone find about 600,000 services, which is only
# 1.5 times the size required for storing two tables
# of 200,000 entries in version 1. We then reduce space
# further by storing fewer data for each entry, which means
# version2 should not use more memory than version 1.
# It should also run faster.

class service_entry:
    def __init__(self, x):
        self.server = x
        self.hit_count = 0
        self.million_hits = 0
        self_million_names = 0
        self.name_count = 0
        self.previous_fqdn = ""
        self.nb_millions = []
        for x in range(0,5):
            self.nb_millions.append(0)

def compare_by_names(item, other):
    if item.name_count < other.name_count:
        return -1
    elif item.name_count > other.name_count:
        return 1
    elif item.hit_count < other.hit_count:
        return -1
    elif item.hit_count > other.hit_count:
        return 1
    elif item.server < other.server:
        return -1
    elif item.server > other.server:
        return 1
    else:
        return 0

class zone_parser2:
    def __init__(self, ps):
        self.sf_dict = dict()
        self.hit_count = 0
        self.name_count = 0
        self.million_names = 0
        self.million_hits = 0
        self.previous_fqdn = ""
        self.approx_servers = hyperloglog.hyperloglog(6)
        self.ps = ps
        self.dups = dict()
        self.millions = dict()
        self.nb_millions = []
        for x in range(0,5):
            self.nb_millions.append(0)

    def load_dups(self, file_name):
        for line in open(file_name , "rt", encoding="utf-8"):
            # parse the input line
            parts = line.split(",")
            if len(parts) >= 2:
                key = parts[0].strip()
                val = parts[1].strip()
                self.dups[key] = val

    def load_million(self, file_name):
        rank = 0
        log_rank = 0
        next_limit = 100
        for line in open(file_name , "rt", encoding="utf-8"):
            million_host = line.strip()
            rank += 1
            if rank > next_limit and next_limit < 1000000:
                log_rank += 1
                next_limit *= 10
            p = million_host.split(".")
            x = ""
            is_suffix = False
            if len(p) == 2:
                x = p[0] + "." + p[1]
                is_suffix = True
            else:
                x,is_suffix = self.ps.suffix(million_host)
            if x != "" and is_suffix and not x in self.millions:
                self.millions[x] = log_rank

    def add(self, ns_name, fqdn):
        million_rank = -1
        if fqdn != self.previous_fqdn:
            self.name_count += 1
            self.previous_fqdn = fqdn
            y = fqdn
            if y.endswith("."):
                y = y[0:-1]
                if y in self.millions:
                    million_rank = self.millions[y]
                    self.nb_millions[million_rank] += 1
                    self.million_names += 1
        self.hit_count += 1
        if million_rank >= 0:
            self.million_hits += 1
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
            if x in self.dups:
                x = self.dups[x]
            if not x in self.sf_dict:
                self.sf_dict[x] = service_entry(x)
            self.sf_dict[x].hit_count += 1
            if fqdn != self.sf_dict[x].previous_fqdn:
                self.sf_dict[x].name_count += 1
                self.sf_dict[x].previous_fqdn = fqdn
                if million_rank >= 0:
                    self.sf_dict[x].nb_millions[million_rank] += 1
        return True

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
        flat = list(self.sf_dict.values())
        flat.sort(key=functools.cmp_to_key(compare_by_names), reverse=True)
        f = open(file_name , "wt", encoding="utf-8")
        f.write("table,server,nb_hits,nb_names," + self.approx_servers.header_full_text("h") + "\n");
        f.write("top,names," + str(self.hit_count) + "," + str(self.name_count) + "\n")
        f.write("top,services," + str(self.hit_count) + "," + str(len(flat)) + "\n")
        f.write("top,million," + str(self.million_hits) + "," + str(self.million_names))
        for r in self.nb_millions:
            f.write("," + str(r))
        f.write("\n")

        x = self.approx_servers.evaluate()
        f.write("top,servers," + str(self.hit_count) + "," + str(x) + "," + self.approx_servers.to_full_text() + "\n")
        for entry in flat:
            f.write("sf," + entry.server + ","  + str(entry.hit_count)  + "," + str(entry.name_count))
            for r in entry.nb_millions:
                f.write("," + str(r))
            f.write("\n")

        f.close()