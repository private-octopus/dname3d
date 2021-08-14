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

# Unified list of prefixes
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

