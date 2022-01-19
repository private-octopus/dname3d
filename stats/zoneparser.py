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
import functools
import ipaddress
import pubsuffix
import os

# partition a file, for exampel so that multiple threads can work on a big zone file.
# partitions end at the closest domain transition.
def compute_file_partitions(file_name, nb_parts):
    # open file
    file = open(file_name, "rt", encoding="utf-8")
    # get the cursor positioned at end
    file.seek(0, os.SEEK_END)
    # get the current position of cursor
    # this will be equivalent to size of file
    file_size = file.tell()
    # split the file in nb_parts
    file_part = [0]
    for x in range(1,nb_parts):
        b = int (x*file_size/nb_parts)
        if b > 512:
            b -= 512
        file.seek(b)
        first_line = True
        name_found = False
        name = ""
        for line in file:
            if first_line:
                first_line = False
            else:
                parts = line.split("\t")
                if len(parts) > 0:
                    name_part = parts[0].strip()
                    if name_found:
                        if name_part != name:
                            break
                    else:
                        name = name_part
                        name_found = True
            b += len(line)
        file_part.append(b)
    file_part.append(file_size)
    file.close()
    return file_part

# Normalise the names of the name servers so we can tabulate them
def extract_server_suffix(ns_name, ps, dups):
    x,is_suffix = ps.suffix(ns_name)
    if x == "":
        np = ns_name.split(".")
        l = len(np)
        while l >= 2 and len(np[l-1]) == 0:
            l -= 1
        if not is_suffix and l > 2:
            np = np[l-2:]
        while i in range(0,len(np)):
            if x != "":
                x += "."
            x += np[i]
        print(ns_name + "maps to " + x + ", is_suffix: " + str(is_suffix))
    if x != "":
        # special rule for AWS DNS
        if x.startswith("awsdns-"):
            p = x.split(".")
            x = "awsdns-??"
            for np in p[1:]:
                x += "." + np
        # other possible duplicate names for same service 
        if x in dups:
            x = dups[x]
    else:
        # special case when no map.
        print("No mapping for: " + ns_name)
    return x

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
            if x == "":
                x = million_host
            if not x in self.millions:
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
        x = extract_server_suffix(ns_name, self.ps, self.dups)
        if x == "":
            print("Cannot add empty suffix for " + ns_name)
        else:
            if not x in self.sf_dict:
                self.sf_dict[x] = service_entry(x)
                if x == "awsdns-??.org":
                    print(ns_name + " matches " + x)
            self.sf_dict[x].hit_count += 1
            if fqdn != self.sf_dict[x].previous_fqdn:
                self.sf_dict[x].name_count += 1
                self.sf_dict[x].previous_fqdn = fqdn
                if million_rank >= 0:
                    self.sf_dict[x].nb_millions[million_rank] += 1
        return True

    def add_zone_file(self, file_name, p_start=0, p_end=0):
        file = open(file_name , "rt", encoding="utf-8")
        file_pos = 0
        if p_start != 0:
            file.seek(p_start)
            file_pos = p_start
        for line in file:
            file_pos += len(line)
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
            if p_end != 0 and file_pos >= p_end:
                break
        file.close()

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

    def parse_line(parts):
        x = []
        for p in parts:
            x.append(int(p))
        return x

    def load_partial_result(self, result_file):
        has_names = False
        has_services = False
        has_million = False
        file = open(result_file , "rt", encoding="utf-8")
        line = file.readline()
        if not line.startswith("table,server,nb_hits,nb_names,"):
            print("File " + result_file + " does not with expected header.")
            return False
        for line in file:
            parts = line.strip().split(",")
            if len(parts) == 0:
                continue
            else:
                try:
                    x = zone_parser2.parse_line(parts[2:])
                    if parts[0] == "top":
                        if parts[1] == "names" and len(x) == 2:
                            self.hit_count += x[0]
                            self.name_count += x[1]
                        elif parts[1] == "services":
                            pass
                        elif parts[1] == "million":
                            if len(x) == 2 + len(self.nb_millions):
                                self.million_hits += x[0]
                                self.million_names += x[1]
                                for i in range(0,len(self.nb_millions)):
                                    self.nb_millions[i] += x[2+i]
                            else:
                                print("File " + result_file + " expexted 2 + " + str(len(self.nb_millions)) + " numbers, got: " + line.strip())
                                return False
                        elif parts[1] == "servers":
                            if len(x) == 2 + self.approx_servers.nb_buckets():
                                self.approx_servers.merge_vector(x[2:])
                            else:
                                print("File " + result_file + " expexted 2 + " + str(self.approx_servers.nb_buckets()) + " numbers, got: " + line.strip())
                                return False
                        else:
                            print("File " + result_file + " unexpexted top line: " + line.strip())
                            return False
                    elif parts[0] == "sf" and len(x) == 2 + len(self.nb_millions):
                        n = parts[1]
                        if not n in self.sf_dict:
                            self.sf_dict[n] = service_entry(n)
                        self.sf_dict[n].hit_count += x[0]
                        self.sf_dict[n].name_count += x[1]
                        for i in range(0,len(self.nb_millions)):
                            self.sf_dict[n].nb_millions[i] += x[2+i]
                    else:
                        # Error!
                        print("File " + result_file + " unexpexted line: " + line.strip())
                        return False
                except Exception as e:
                    traceback.print_exc()
                    print("Cannot parse line: " + line.strip() + "\n in file <" + result_file  + ">\nException: " + str(e))
                    return False
        return True

