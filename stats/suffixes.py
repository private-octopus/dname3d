#!/usr/bin/python
# coding=utf-8
#
# Parse the names file

import traceback
import nameparse
import hyperloglog
import gzip
import traceback
import functools
import sys
import ipaddress

# Unified list of suffixes
#
# There is a name file for each instance and each slice of time. The file contains lists of names,
# counts and attributes. The code assumes that a first pass is done to only retain the valid names.
# The purpose of the code is to find common prefixes, and to assess how many names are registered
# under each prefix. We do this using the hyperloglog algorithm, which provides as statistical
# estimate. The expected operation are as follow:
#
# 1- In a first pass, obtain names from name files and create a summary of N files. For each
#    name, retain a hyperloglog object and a hit count, indicating how many names have been
#    found with this prefix. The process is recursive. For example, an entry to
#    "some.example.com 25" will add the prefix "some" into "example.com" with count 25,
#    and the prefix "example" under "com" with count 0. A direct hit to "com, 17" will
#    add 17 to the hit count for "com", but no prefix.
# 2- At the end of the first pass, save the summary file.
# 3- In a second pass, retrieve all the name summaries, and add them in a join summary file.
#    When a prefix is found in multiple files, the hyperloglog informations are merged,
#    and the hit count are added.
# 4- The save function may include a filtering option, either based on number of hits or on
#    number of names. If a prefix is filtered, the hit count for that prefix are added
#    to the next level, or to the artificial prefix "__other_tlds".

class suffix_summary_entry:
    def __init__(self, suffix, hits, hll_m):
        self.suffix = suffix
        self.hits = hits
        self.subs = 0
        self.hll = hyperloglog.hyperloglog(hll_m)
    def add_subname(self, name_part, hits):
        if name_part != "":
            self.hll.add(name_part)
        self.hits += hits
        self.subs = 0
    def merge(self, other):
        self.hits += other.hits
        self.hll.merge(other.hll)
        self.subs = 0
    def evaluate(self):
        if self.subs == 0:
            self.subs = self.hll.evaluate()
        return self.subs
    def to_text(self):
        s = self.suffix + ","
        s += str(self.hits) + ","
        s += str(self.evaluate()) + ","
        s += self.hll.to_text()
        return s
    def from_text(self, s):
        success = True
        try:
            p = s.split(",")
            self.suffix = p[0].strip()
            self.hits = int(p[1])
            self.subs = int(p[2])
            self.hll.from_parts(p[3:])
        except:
            success = False
        return success

def compare_by_subs(item, other):
    n1 = item.evaluate()
    n2 = other.evaluate()
    if n1 < n2:
        return -1
    elif n1 > n2:
        return 1
    elif item.suffix < other.suffix:
        return -1
    elif item.suffix > other.suffix:
        return 1
    else:
        return 0

class suffix_summary_file:
    def __init__(self, hll_m, max_suffix_parts):
        self.summary = dict()
        self.hll_m = hll_m
        self.max_suffix_parts = max_suffix_parts
    
    def add_name(self, name, hits):
        already_seen = False
        # add_name is used in the first pass, to accumulate hits from a name file.
        name_parts = name.split(".")
        np = len(name_parts)
        i_sfn = 0
        i_start = 0
        # trim name so it be at most max_suffix_parts + 1
        while i_start < np and np-i_start > self.max_suffix_parts + 1:
            i_sfn += len(name_parts[i_start])+1
            i_start += 1
        # if name is short, record it
        if hits > 0 and np-i_start <= self.max_suffix_parts:
            suffix = name[i_sfn:]
            if suffix in self.summary:
                # if the name is already, no need for further processing.
                self.summary[suffix].hits += hits
                already_seen = False
            else:
                self.summary[suffix] = suffix_summary_entry(suffix, hits, self.hll_m)
            hits = 0
        # if there are unrecorded suffix parts, record them
        if not already_seen:
            for i_p in range(i_start,np-1):
                i_sfn += len(name_parts[i_p])+1
                suffix = name[i_sfn:]
                if suffix in self.summary:
                    # if the suffix is already accounted for, the shorter prefixes
                    # are up to date, so processing will stop there
                    self.summary[suffix].add_subname(name_parts[i_p],hits)
                    break
                else:
                    # if creating a prefix, continue this loop so shorter prefixes
                    # are updated as needed.
                    self.summary[suffix] = suffix_summary_entry(suffix, 0, self.hll_m)
                    self.summary[suffix].add_subname(name_parts[i_p],hits)
                    hits = 0

    def prune(self, min_hits, min_subnames):
        suffixes = list(self.summary.keys())
        kept = dict()
        leaked = 0
        for n in range(self.max_suffix_parts, 1, -1):
            for s in suffixes:
                p = s.split(".")
                np = len(p)
                if np == n and not s in kept:
                    sse = self.summary[s]
                    should_keep = (min_hits > 0 and sse.hits >= min_hits) or (min_subnames > 0 and sse.hll.evaluate() >= min_subnames)
                    if should_keep:
                        if n > 0:
                            isf = 0
                            for ip in range(1, np-1):
                                isf += len(p[ip-1]) + 1
                                kept[s[isf:]] = 1
                    else:
                        if n > 0:
                            isf = len(p[0]) + 1
                            self.summary[s[isf:]].hits += sse.hits
                        self.summary.pop(s)

    def parse_suffix_summary(self, file_name):
        for line in open(file_name , "rt", encoding="utf-8"):
            sse = suffix_summary_entry("", 0, self.hll_m)
            if sse.from_text(line):
                if sse.suffix in self.summary:
                    self.summary[sse.suffix].merge(sse)
                else:
                    self.summary[sse.suffix] = sse

    def evaluate(self):
        for suffix in self.summary:
            self.summary[suffix].evaluate()

    def save_list(flat, file_name):
        f = open(file_name , "wt", encoding="utf-8")
        for sse in flat:
            f.write(sse.to_text() + '\n')
        f.close()
                    
    def save_suffix_summary(self, file_name, top_n=0, sort=False, by_hits=False, eval=False):
        print("Save summary, top_n = ", + top_n)
        if eval:
            self.evaluate()
        flat = list(self.summary.values())
        if top_n > 0 or sort:
            print("sorting")
            if by_hits:
                flat.sort(key=hits, reverse=True)
            else:
                flat.sort(key=functools.cmp_to_key(compare_by_subs), reverse=True)
            print("sorted")
        if top_n == 0:
            top_n = len(flat)
        print("saving " + str(top_n))
        suffix_summary_file.save_list(flat[0:top_n], file_name)
        print("saved")

    def top_by_hits(self, file_name, nb_requested):
        self.save_suffix_summary(file_name, top_n=nb_requested, sort=True, eval=True, by_hits=True)

    def top_by_subs(self, file_name, nb_requested=0):
        self.save_suffix_summary(file_name, top_n=nb_requested, sort=True, eval=True)

class suffix_summary_sorter:
    def __init__(self, hll_m, top_n):
        self.hll_m = hll_m
        self.top_n = top_n
        self.total = 0
        self.counter = 0
        self.in_file = []
        self.summary = dict()
        self.summary_min = 0

    def sort_and_filter(self):       
        if self.need_sort:
            self.list.sort(key=functools.cmp_to_key(compare_by_subs), reverse=True)

    def add_file_entries_to_summary(self):
        # First add the summary to the in_file list
        for suffix in self.summary:
            self.in_file.append(self.summary[suffix])
        # Sort the file list
        self.in_file.sort(key=functools.cmp_to_key(compare_by_subs), reverse=True)
        # Trim to top N
        if self.top_n > 0:
            self.in_file = self.in_file[0:self.top_n]
        # Reset the summary
        self.summary = dict()
        if self.top_n <= 0 or len(self.summary) < self.top_n or len(self.in_file) == 0:
            self.summary_min = 0
        else:
            self.summary_min = self.in_file[-1].subs
        for sse in self.in_file:
            self.summary[sse.suffix] = sse
        # Reset the input file
        self.in_file = []
        self.counter = 0

    def parse_suffix_summary(self, file_name):
        # For all the names in the list, check whether they are already
        # part of the summary, and merge them if they are.
        # If they are not, and if they are higher than the lowest in 
        # the summary, retain them in a top 10 list.
        self.in_file=[]
        self.counter = 0

        for line in open(file_name , "rt", encoding="utf-8"):
            sse = suffix_summary_entry("", 0, self.hll_m)
            if sse.from_text(line):
                self.total += 1
                if sse.suffix in self.summary:
                    self.summary[sse.suffix].merge(sse)
                    self.summary.evaluate()
                else:
                    sse.evaluate()
                    if sse.subs >= self.summary_min:
                        self.in_file.append(sse)
                        self.counter += 1
                        if self.top_n > 0 and self.counter > 2*self.top_n:
                            self.add_file_entries_to_summary()
                            sys.stdout.write("+")
                            sys.stdout.flush()
        # End of the loop. Need to add the in_file entries to the summary
        if len(self.in_file) > 0:
            self.add_file_entries_to_summary()
            sys.stdout.write("!")
            sys.stdout.flush()
        print("\nAdded " + file_name + ", total: " + str(self.total))

    def save_summary(self, file_name):
        top_n = self.top_n
        print("Save summary, top_n = " + str(top_n))
        flat = list(self.summary.values())
        if top_n > 0:
            print("sorting")
            flat.sort(key=functools.cmp_to_key(compare_by_subs), reverse=True)
            print("sorted")
        if top_n == 0:
            top_n = len(self.list)
        print("saving " + str(self.top_n))
        suffix_summary_file.save_list(self.list[0:top_n], file_name)
        print("saved")


# Detailed analysis of suffixes
# 
# The suffix summaries above provide a first pass evaluation of suffixes.
# This firt pass is used to extract a list of "top N" suffixes, for which more data
# is collected, ending up with one line per instance and day of data collection:
#
# * value of suffix
# * total number of references to names within that suffix (as in summary)
# * estimated number of sub-domains for the suffix (as in summary)
# * estimated number of IP addresses for the suffix
# * estimated number of subnets (/24 or /48) for the suffix
#
# Each estimated number is represented by an HyperLogLog object, and
# HyperLogLog logic is used when computing aggregates.
#
# This is computed by first computing details for each instance, and then
# obtaining a summary file.
#
# There is a plausible discussion of whether to aggregate by city or country
# rather than instance. Cities and countries can be parsed from the
# instance name, so this can be done during the aggregation phase.
#
# There is also a discussion of whether we want 2 phases or single phase.
# Single phase is much better for operational purposes. Just process files
# for each instance every day. Once processed, all name stats can be drown
# from there. But we get an issue of precision. We want eventually to list
# traffic for the top N suffixes for a month, but we don't know which these
# N suffixes will be. In two passes, we know and we can count. In one pass,
# we could try to keep everything, but that requires a whole lot of
# bandwidth. Or we could for each day keep the top-X, with X >> N.
#
# In practice, setting X to a few thousands and N to a few hundreds appears
# practical. But if we keep the first few thousands, we have two plausible
# metrics: more hits, or more subs. Keeping the most subs appears simpler.
#
# In usage, we start with parallel processing of all recodings for an
# instance and a day. There are 24*12 = 288 slices per day, and the
# top N for each slice could be very different if there is time
# dependency. We want to keep many slices. So maybe in the first pass
# we keep everything -- but we find a way to eliminate noise. 
#
# But then, do we need a new class for that? If just one pass, then
# why not just add the IP address statistics to the main file? This
# would benefit from keeping the tests and the scripts unchanged.
# 

class suffix_detail_entry:
    def __init__(self, suffix, hll_k):
        self.suffix = suffix
        self.hits = 0
        self.subs = 0
        self.ips = 0
        self.nets = 0
        self.sub_hll = hyperloglog.hyperloglog(hll_k)
        self.ip_hll = hyperloglog.hyperloglog(hll_k)
        self.net_hll = hyperloglog.hyperloglog(hll_k)

    def add_subname(self, name_part, hits, ip):
        if name_part != "":
            self.sub_hll.add(name_part)
            self.subs = 0
        ip_is_bad = False
        ipa = ipaddress.ip_address(ip)
        if ipa.version == 4:
            isn = ipaddress.ip_network(ip + "/24", strict=False)
        elif ipa.version == 6:
            isn = ipaddress.ip_network(ip + "/48", strict=False)
        else:
            isn = ipaddress.ip_network("::/64")
        if not ip_is_bad:
            self.ip_hll.add(str(ipa))
            self.net_hll.add(str(isn))
            self.ips = 0
            self.nets = 0
        self.hits += hits

    def merge(self, other):
        self.hits += other.hits
        self.sub_hll.merge(other.sub_hll)
        self.ip_hll.merge(other.ip_hll)
        self.net_hll.merge(other.net_hll)
        self.subs = 0
        self.ips = 0
        self.nets = 0
    def evaluate(self):
        if self.subs == 0:
            self.subs = self.sub_hll.evaluate()
        if self.ips == 0:
            self.ips = self.ip_hll.evaluate()
        if self.nets == 0:
            self.nets = self.net_hll.evaluate()
    def suffix_header(self):
        s = "Suffix,Hits,Subs,IPs,Nets,"
        s += self.sub_hll.header_full_text("S") + ","
        s += self.ip_hll.header_full_text("I") + ","
        s += self.ip_hll.header_full_text("N")
        return s
    def to_text(self):
        self.evaluate()
        s = self.suffix + ","
        s += str(self.hits) + ","
        s += str(self.subs) + ","
        s += str(self.ips) + ","
        s += str(self.nets) + ","
        s += self.sub_hll.to_full_text() + ","
        s += self.ip_hll.to_full_text() + ","
        s += self.net_hll.to_full_text()
        return s
    def from_text(self, s):
        success = True
        try:
            p = s.split(",")
            self.suffix = p[0].strip()
            self.hits = int(p[1])
            self.subs = int(p[2])
            self.ips = int(p[3])
            self.nets = int(p[4])
            ix = 5
            self.sub_hll.from_full_parts(p[ix:])
            ix += self.sub_hll.m
            self.ip_hll.from_full_parts(p[ix:])
            ix += self.ip_hll.m
            self.net_hll.from_full_parts(p[ix:])
        except:
            success = False
        return success

def compare_suffix_details(item, other):
    item.evaluate()
    other.evaluate()
    r = 0
    if item.subs < other.subs:
        r = -1
    elif item.subs > other.subs:
        r = 1
    elif item.nets < other.nets:
        r = -1
    elif item.nets > other.nets:
        r = 1
    elif item.ips < other.ips:
        r = -1
    elif item.ips > other.ips:
        r = 1
    elif item.hits < other.hits:
        r = -1
    elif item.hits > other.hits:
        r = 1
    elif item.suffix < other.suffix:
        r = -1
    elif item.suffix > other.suffix:
        r = 1
    else:
        r = 0
    return r

# Extract details from name files and a list of suffixes.
#
# If this is just a second pass, the list of suffixes is preset by
# a call to `init_suffixes`. If it is not, then all the prefixes
# are loaded in memory, but only the top 10,000 will be stored.

class suffix_details_file:
    def __init__(self, hll_k, max_suffix_parts):
        self.suffixes = dict()
        self.hll_k = hll_k
        self.max_suffix_parts = max_suffix_parts
        self.dynamic_list = True

    def init_suffixes(self, suffix_list):
        for suffix in suffix_list:
            self.suffixes[suffix] = suffix_detail_entry(suffix, self.hll_k)
        self.dynamic_list = False

    def add_to_suffix(self, suffix, subname, hits, ip):
        if suffix in self.suffixes:
            self.suffixes[suffix].add_subname(subname, hits, ip)
        elif self.dynamic_list:
            self.suffixes[suffix] = suffix_detail_entry(suffix, self.hll_k)
            self.suffixes[suffix].add_subname(subname, hits, ip)

    def add_name(self, name, hits, ip):
        name_parts = name.split(".")
        np = len(name_parts)
        i_sfn = 0
        i_start = 0

        # if name is short, add an empty name, which means counting
        # just the IP and subnet hits.
        if np <= self.max_suffix_parts:
            self.add_to_suffix(name, "", hits, ip)

        else:
            # trim name so it be at most max_suffix_parts + 1
            while i_start + self.max_suffix_parts + 1 < np:
                i_sfn += len(name_parts[i_start])+1
                i_start += 1
        # Record all the embedded suffixes
        while i_start + 1 < np:
            i_sfn += len(name_parts[i_start])+1
            suffix = name[i_sfn:]
            self.add_to_suffix(suffix, name_parts[i_start], hits, ip)
            i_start += 1

    def evaluate(self):
        for suffix in self.suffixes:
           self.suffixes[suffix].evaluate()

    def top_n(self, nb_top):
        suffix_list = list(self.suffixes.values())
        suffix_list.sort(key=functools.cmp_to_key(compare_suffix_details), reverse=True)
        if self.dynamic_list and len(suffix_list) > nb_top:
            suffix_list = suffix_list[0:nb_top]
        return suffix_list

    def trim(self, nb_top):
        if len(self.suffixes) > nb_top:
            suffix_list = self.top_n(nb_top)
            trimmed = dict()
            for sde in suffix_list:
                trimmed[sde.suffix] = sde
            self.suffixes = trimmed

    def save(self, file_name):
        # start with sorting by relevance, then limit
        # to a maximum size of 10,000
        suffix_list = self.top_n(10000)
        with open(file_name , "wt", encoding="utf-8") as f:
            if len(suffix_list) > 0:
                f.write(suffix_list[0].suffix_header() + "\n")
            for suffix in suffix_list:
                f.write(suffix.to_text() + "\n")

    def parse(self, file_name):
        for line in open(file_name , "rt", encoding="utf-8"):
            sde = suffix_detail_entry("", self.hll_k)
            if sde.from_text(line):
                if sde.suffix in self.suffixes:
                    self.suffixes[sde.suffix].merge(sde)
                else:
                    self.suffixes[sde.suffix] = sde

    def merge(self, other):
        for suffix in other.suffixes:
            if suffix in self.suffixes:
                self.suffixes[suffix].merge(other.suffixes[suffix])
            else:
                self.suffixes[suffix] = other.suffixes[suffix]


# Prepare monthly per instance daily reports.
#
# This is done by aggregating multiple files corresponding to 
# the same instance but multiple days.

class suffix_report:
    def __init__(self, hll_k, max_suffix_parts, nb_top):
        self.hll_k = hll_k
        self.max_suffix_parts = max_suffix_parts
        self.nb_top = nb_top
        self.top_list = suffix_details_file(hll_k, max_suffix_parts)
        self.date_list = dict()
        self.city_list = dict()

    def get_city_date_from_file_name(file_name):
        # Expect file name such as aa01-tw-ntc-suffixes-20210721.csv.
        # Parse the names to extract city and date
        p0 = file_name.split("/")
        if len(p0) <= 1:
            # This can happen if running on windows
            p0 = file_name.split("\\")
        p1 = p0[-1].split(".")
        parts = p1[0].split("-")
        date = parts[4].strip()
        city = parts[1] + "-" + parts[2]
        return date,city

    def set_city_date_lists(self, file_list):
        for file_name in file_list:
            # Expect file name such as aa01-tw-ntc-suffixes-20210721.csv.
            # Parse the names to extract city and date
            date,city = suffix_report.get_city_date_from_file_name(file_name)
            if not date in self.date_list:
                self.date_list[date] = suffix_details_file(self.hll_k, self.max_suffix_parts)
            if not city in self.city_list:
                self.city_list[city] = suffix_details_file(self.hll_k, self.max_suffix_parts)

    def compute_city_or_date_report(hll_k, max_suffix_parts, city, date, file_list, output_file):
        details_file = suffix_details_file(hll_k, max_suffix_parts)
        for file_name in file_list:
            f_date,f_city = suffix_report.get_city_date_from_file_name(file_name)
            if (city != "" and f_city == city) or (date != "" and f_date == date):
                details_file.parse(file_name)
        details_file.save(output_file)
   
    def get_top_domains(self):
        self.top_list = suffix_details_file(self.hll_k, self.max_suffix_parts)
        for date in self.date_list:
            print("Merging " + date)
            self.top_list.merge(self.date_list[date])
        self.top_list.trim(self.nb_top)

    def write_suffix_date(f, date, city, sde):
        sde.evaluate()
        f.write(sde.suffix + "," + date + "," + city + "," + str(sde.hits) \
            + "," + str(sde.subs) + "," + str(sde.ips) + "," + str(sde.nets) + "\n")

    def save_top_details(self, file_name):
        with open(file_name , "wt", encoding="utf-8") as f:
            f.write("Suffix,date,city,hits,subs,ips,nets\n")
            for suffix in self.top_list.suffixes:
                suffix_report.write_suffix_date(f, "all", "all", self.top_list.suffixes[suffix]);
                for date in self.date_list:
                    dld = self.date_list[date]
                    if suffix in dld.suffixes:
                        suffix_report.write_suffix_date(f, date, "all", dld.suffixes[suffix]);
                for city in self.city_list:
                    cld = self.city_list[city]
                    if suffix in cld.suffixes:
                        suffix_report.write_suffix_date(f, "all", city, cld.suffixes[suffix]);
