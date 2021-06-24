#!/usr/bin/python
# coding=utf-8
#
# Parse the names file

import traceback
import nameparse
import gzip
import traceback

class prefixline:
    def __init__(self):
        self.prefix = ""
        self.sub_count = 0
        self.hit_count = 0
        self.nb_parts = 0

    def copy(self):
        cp = prefixline()
        cp.prefix = self.prefix
        cp.sub_count = self.sub_count
        cp.hit_count = self.hit_count
        cp.nb_parts = self.nb_parts
        return cp

    def from_csv(self, line):
        ret = False
        p = line.split(",")
        if len(p) == 4:
            self.prefix = p[0]
            try:
                self.nb_parts = int(p[3])
            except:
                self.nb_parts = 0
            try:
                self.sub_count = int(p[1])
            except:
                self.sub_count = 0
            try:
                self.hit_count = int(p[2])
                if self.hit_count > 0:
                    ret = True
            except:
                self.hit_count = 0
        return ret

    def to_csv(self):
        t = self.prefix + "," + str(self.sub_count) + "," + str(self.hit_count) + "," + str(self.nb_parts) +"\n"
        return t

    def csv_head():
        return("prefix, sub_count, hit_count, nb_parts\n")

# The prefix list accumulates the frequency of prefixes in the input files.
# The raw entries are files of names, with an occurence count indicating how many
# times that name was queried.
# Upon finding a name, the following happen:
# 1) If the name is longer than the maximum depth, shorten it.
# 2) If the name is already present in the prefix list, simply increment the hit count.
# 3) If the name is not present, record it. Initialize the sub count to 0. Initialize the
#    hit count to the specified value. If the prefix length is > 1, increment the
#    sub count of the parent prefix.

class prefixlist:
    def __init__(self, depth):
        self.list = dict()
        self.depth = depth

    def add_line(self, line):
        pl = prefixline()
        if pl.from_csv(line):
            if pl.hit_count > 0:
                self.load_name(pl.prefix, pl.hit_count)

    def load_prefix_file(self, prefix_file):
        for line in open(prefix_file , "rt"):
            self.add_line(line)

    def load_name(self, name, hit_count):
        parts = name.split(".")
        nb_parts = len(parts)
        # remove final dot if any
        while nb_parts > 0 and len(parts[nb_parts-1]) == 0:
            nb_parts -= 1
        if nb_parts > 0 and not parts[nb_parts -1] == "ARPA":
            # trim name to specified depth
            if nb_parts > self.depth:
                parts = parts[len(parts) - self.depth:]
                nb_parts = self.depth
            # compute prefix
            prefix = parts[0]
            for part in parts[1:]:
                prefix += "." + part
            # check whether the prefix is already present
            if prefix in self.list:
                # if present, just increase the hit count
                self.list[prefix].hit_count += hit_count
            else:
                # if absent, add to the list
                pl = prefixline()
                pl.prefix = prefix
                pl.hit_count = hit_count
                pl.nb_parts = nb_parts
                self.list[prefix] = pl
                # increment sub_count of parent
                if nb_parts > 1:
                    parent = parts[1]
                    for part in parts[2:]:
                        parent += "." + part
                    if not parent in self.list:
                        # Add an entry for the parent, and recurse
                        self.load_name(parent, 0)
                    self.list[parent].sub_count += 1

    def load_logfile_csv(self, logfile):
        for line in open(logfile , "rt"):
            nl = nameparse.nameline()
            if nl.from_csv(line) and nl.name_type =="tld":
                self.load_name(nl.name, nl.count)

    def load_logfile_gz(self, logfile):
        try:
            with gzip.open(logfile,'rt') as fin: 
                for line in fin:
                    nl = nameparse.nameline()
                    if nl.from_csv(line) and nl.name_type =="tld":
                        self.load_name(nl.name, nl.count)
        except Exception as e:
            traceback.print_exc()
            print("Cannot process compressed file <" + logfile  + ">\nException: " + str(e))
            print("Giving up");
            exit(1) 

    def load_logfile(self, logfile):
        if logfile.endswith(".gz"):
            self.load_logfile_gz(logfile)
        else:
            self.load_logfile_csv(logfile)

    def write_file(self,logfile):
        f = open(logfile , "wt", encoding="utf-8")
        f.write(prefixline.csv_head())
        for prefix in self.list:
            f.write(self.list[prefix].to_csv())
        f.close()

    def has_dga13(self):
        dga13_count = 0
        tld_count = 0
        for prefix in self.list:
            is_dga = False
            pv = self.list[prefix]
            if pv.nb_parts == 2 and pv.sub_count == 0 and pv.hit_count == 1:
                parts = prefix.split(".")
                is_dga = len(parts) == 2 and (len(parts[0]) == 13 or len(parts[0]) == 12)
            if is_dga:
                dga13_count += 1
            else:
                tld_count += 1
                if tld_count > 10000:
                    break
        return tld_count < 10*dga13_count

    def purge_dga13(self):
        purged_list = prefixlist(self.depth)
        for prefix in self.list:
            is_dga = False
            pv = self.list[prefix]
            if pv.nb_parts == 2 and pv.sub_count == 0 and pv.hit_count == 1:
                parts = prefix.split(".")
                is_dga = len(parts) == 2 and (len(parts[0]) == 13 or len(parts[0]) == 12)
                if is_dga:
                    dga_prefix = "__dga13__." + parts[1]
                    purged_list.load_name(dga_prefix, pv.hit_count)
            if not is_dga:
                purged_list.load_name(prefix, pv.hit_count)
        return purged_list

class prefixbranch:
    def __init__(self):
        self.branches = dict()
        self.hit_count = 0

    def write_branch(self, f, nb_parts, suffix):
        sub_count = len(self.branches)
        if len(self.branches) > 0:
            s = ""
            if nb_parts > 0:
                s += "."
            s += suffix
            for name_part in self.branches:
                self.branches[name_part].write_branch(f, nb_parts+1, name_part + s)
        if nb_parts > 0:
            f.write(suffix + "," + str(sub_count) + "," + str(self.hit_count) + "," + str(nb_parts) + "\n")

class prefixtree:
    def __init__(self, depth):
        self.root = prefixbranch()
        self.depth = depth

    def load_name(self, name, hit_count):
        parts = name.split(".")
        nb_parts = len(parts)
        # remove final dot if any
        while nb_parts > 0 and len(parts[nb_parts-1]) == 0:
            nb_parts -= 1
        if nb_parts > 0 and not parts[nb_parts -1] == "ARPA":
            # trim name to specified depth
            if nb_parts > self.depth:
                parts = parts[len(parts) - self.depth:]
                nb_parts = self.depth
            branch = self.root
            while nb_parts > 0:
                nb_parts -= 1
                part = parts[nb_parts]
                if not part in branch.branches:
                    branch.branches[part] = prefixbranch()
                if nb_parts == 0:
                    branch.branches[part].hit_count += hit_count
                else:
                    branch = branch.branches[part]

    def load_prefix_file(self, prefix_file):
        for line in open(prefix_file , "rt"):
            pl = prefixline()
            if pl.from_csv(line):
                if pl.hit_count > 0:
                    self.load_name(pl.prefix, pl.hit_count)
                    
    def load_logfile_csv(self, logfile):
        for line in open(logfile , "rt"):
            nl = nameparse.nameline()
            if nl.from_csv(line) and nl.name_type =="tld":
                self.load_name(nl.name, nl.count)

    def load_logfile_gz(self, logfile):
        try:
            with gzip.open(logfile,'rt') as fin: 
                for line in fin:
                    nl = nameparse.nameline()
                    if nl.from_csv(line) and nl.name_type =="tld":
                        self.load_name(nl.name, nl.count)
        except Exception as e:
            traceback.print_exc()
            print("Cannot process compressed file <" + logfile  + ">\nException: " + str(e))
            print("Giving up");
            exit(1) 

    def load_logfile(self, logfile):
        if logfile.endswith(".gz"):
            self.load_logfile_gz(logfile)
        else:
            self.load_logfile_csv(logfile)

    def write_file(self,logfile):
        f = open(logfile , "wt", encoding="utf-8")
        f.write(prefixline.csv_head())
        self.root.write_branch(f, 0, "")
        f.close()


