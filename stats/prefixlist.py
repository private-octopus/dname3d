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

    def myComp(self, other):
        if (self.sub_count < other.sub_count):
            return -1
        elif (self.sub_count > other.sub_count):
            return 1
        elif (self.nb_parts < other.nb_parts):
            return -1
        elif (self.nb_parts > other.nb_parts):
            return 1
        elif (self.hit_count < other.hit_count):
            return -1
        elif (self.hit_count > other.hit_count):
            return 1
        elif (self.prefix < other.prefix):
            return -1
        elif (self.prefix > other.prefix):
            return 1
        else:
            return 0
    
    def __lt__(self, other):
        return self.myComp(other) < 0
    def __gt__(self, other):
        return self.myComp(other) > 0
    def __eq__(self, other):
        return self.myComp(other) == 0
    def __le__(self, other):
        return self.myComp(other) <= 0
    def __ge__(self, other):
        return self.myComp(other) >= 0
    def __ne__(self, other):
        return self.myComp(other) != 0

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
            if pv.nb_parts >= 2 and pv.sub_count == 0 and pv.hit_count == 1:
                parts = prefix.split(".")
                is_dga = len(parts) >= 2 and (len(parts[0]) == 13 or len(parts[0]) == 12)
                if is_dga:
                    dga_prefix = "__dga13-" + str(len(parts)) + "_." + parts[1]
                    purged_list.load_name(dga_prefix, pv.hit_count)
            if not is_dga:
                purged_list.load_name(prefix, pv.hit_count)
        return purged_list

    # TODO: for the top prefixes, we should really compute the total hits
    # on that prefix, not just the direct hits.
    def top_prefixes(self, file_name, max_count):
        flat = list(self.list.values())
        flat.sort(reverse=True)
        nb_published = 0
        f = open(file_name , "wt", encoding="utf-8")
        f.write(prefixline.csv_head())
        for pv in flat:
            if pv.sub_count <= 2:
                break
            f.write(pv.to_csv())
            nb_published += 1
            if max_count > 0 and nb_published >= max_count:
                break
        f.close()

# Prefix summary class is used to accumulated prefixes
# collected during multiple parsing events. The precise count of subdomains is
# impractical, so we tabulate the min and max found in the set of files,
# as well as the number of files in which the prefix is found and the
# name of the file with the highest subdomian found.

class prefix_summary_line:
    def __init__(self, prefix, nb_parts, hit_count, sub_min, sub_max, nb_files, file):
        self.prefix = prefix
        self.nb_parts = nb_parts
        self.hit_count = hit_count
        self.sub_min = sub_min
        self.sub_max = sub_max
        self.nb_files = nb_files
        self.file = file

    def add_one(self, hit_count, sub_count, file):
        self.hit_count = hit_count
        if self.sub_min > sub_count:
            self.sub_min = sub_count
        if self.sub_max < sub_count:
            self.sub_max = sub_count
            self.file = file
        self.nb_files += 1

    def add_other(self, other):
        self.hit_count += other.hit_count
        if self.sub_min > other.sub_min:
            self.sub_min = other.sub_min
        if self.sub_max < other.sub_max:
            self.sub_max = other.sub_max
            self.file = other.file
        self.nb_files += other.nb_files
    
    def csv_head():
        s = "prefix"
        s += ", nb_parts"
        s += ", hit_count"
        s += ", sub_min"
        s += ", sub_max"
        s += ", nb_files"
        s += ", file"
        s += "\n"
        return s

    def to_csv(self):
        s = self.prefix
        s += "," + str(self.nb_parts)
        s += "," + str(self.hit_count)
        s += "," + str(self.sub_min)
        s += "," + str(self.sub_max)
        s += "," + str(self.nb_files)
        s += "," + self.file
        s += "\n"
        return s

    def from_csv(line):
        p = line.split(",")
        hit_count = 0
        if len(p) == 8:
            prefix = p[0]
            try:
                hit_count = int(p[2])
                nb_parts = int(p[3])
                sub_min = int(p[4])
                sub_max = int(p[5])
                nb_files = int(p[6])
                file = p[7].strip()
            except:
                hit_count = 0
            
        if hit_count > 0:
            return prefix_summary_line(prefix, nb_parts, hit_count, sub_min, sub_max, nb_files, file)
        else:
            return None

    def myComp(self, other):
        if (self.sub_max < other.sub_max):
            return -1
        elif (self.sub_max > other.sub_max):
            return 1
        if (self.nb_files < other.nb_files):
            return -1
        elif (self.nb_files > other.nb_files):
            return 1
        if (self.sub_min < other.sub_min):
            return -1
        elif (self.sub_min > other.sub_min):
            return 1
        elif (self.nb_parts < other.nb_parts):
            return -1
        elif (self.nb_parts > other.nb_parts):
            return 1
        elif (self.hit_count < other.hit_count):
            return -1
        elif (self.hit_count > other.hit_count):
            return 1
        elif (self.prefix < other.prefix):
            return -1
        elif (self.prefix > other.prefix):
            return 1
        else:
            return 0
    
    def __lt__(self, other):
        return self.myComp(other) < 0
    def __gt__(self, other):
        return self.myComp(other) > 0
    def __eq__(self, other):
        return self.myComp(other) == 0
    def __le__(self, other):
        return self.myComp(other) <= 0
    def __ge__(self, other):
        return self.myComp(other) >= 0
    def __ne__(self, other):
        return self.myComp(other) != 0

class prefix_summary:
    def __init__(self):
        self.summary = dict()

    def add_one(self, prefix, hit_count, sub_count, file):
        if prefix in self.summary:
            self.summary[prefix].add_one(hit_count, sub_count, file)
        else:
            self.summary[prefix] = prefix_summary_line( prefix, len(prefix.split(".")), hit_count, sub_count, sub_count, 1, file)

    def parse_prefixes(self, file_name):
        for line in open(file_name , "rt", encoding="utf-8"):
            pl = prefixline()
            if pl.from_csv(line):
                self.add_one(pl.prefix, pl.hit_count, pl.sub_count, file_name)

    def parse_prefix_summary(self, file_name):
        for line in open(file_name , "rt", encoding="utf-8"):
            pse = prefix_summary_line.from_csv(line)
            if pse != None:
                if pse.prefix in self.summary:
                    self.summary[pse_prefix].add_other(pse)
                else:
                    self.summary[pse_prefix] = pse

    def save_prefix_summary(self, file_name, max_count):
        flat = list(self.summary.values())
        flat.sort(reverse=True)
        nb_published = 0
        f = open(file_name , "wt", encoding="utf-8")
        f.write(prefix_summary_line.csv_head())
        for pse in flat:
            f.write(pse.to_csv())
            nb_published += 1
            if max_count > 0 and nb_published >= max_count:
                break
        f.close()