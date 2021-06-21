#!/usr/bin/python
# coding=utf-8
#
# Parse the names file and extract statistics similar to sum3,
# but also including the dga13 category.

import traceback
import nameparse
import gzip
import traceback

def add_to_list(table, name, count):
    if name in table:
        table[name] += count
    else:
        table[name] = count

class namestats:
    def __init__(self):
        #self.list = dict()
        self.sum_by_cat = dict()
        self.maybe_dga = dict()
        self.dga_tld = dict()
        self.dga_sample = []

    def final_dga(self):
        for name in self.maybe_dga:
            count = self.maybe_dga[name]
            if count != 1:
                add_to_list(self.sum_by_cat, "tld", count)
                # add_to_list(self.list, name, count)
            else:
                add_to_list(self.sum_by_cat, "dga13", count)
                parts = name.split(".")
                if len(parts) == 2:
                    add_to_list(self.dga_tld, parts[1], count)
                    if len(self.dga_sample) < 1000:
                        self.dga_sample.append(name)
        self.maybe_dga = dict()

    def load_name(self, name, count):
        parts = name.split(".")
        nb_parts = len(parts)
        # remove final dot if any
        while nb_parts > 0 and len(parts[nb_parts-1]) == 0:
            nb_parts -= 1
        # TODO: trim to specified depth
        if nb_parts > 0 and parts[nb_parts -1] == "ARPA":
            add_to_list(self.sum_by_cat, "arpa", count)
        elif nb_parts == 2 and (len(parts[0]) == 12 or len(parts[0]) == 13):
            add_to_list(self.maybe_dga, name, count)
        else:
            add_to_list(self.sum_by_cat, "tld", count)

    def load_logline(self, line):
        nl = nameparse.nameline()
        if nl.from_csv(line):
            if nl.name_type == "tld":
                self.load_name(nl.name, nl.count)
            else:
                add_to_list(self.sum_by_cat, nl.name_type, nl.count)

    def load_logfile_csv(self, logfile):
        for line in open(logfile , "rt"):
            self.load_logline(line)

    def load_logfile_gz(self, logfile):
        try:
            with gzip.open(logfile,'rt') as fin: 
                for line in fin:
                    self.load_logline(line)
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

    def export_result_file(self, result_file):
        #self.list = dict()
        f = open(result_file , "wt", encoding="utf-8")
        for cat in self.sum_by_cat:
            f.write("catsum," + cat + "," + str(self.sum_by_cat[cat]) + "\n")
        for dga in self.maybe_dga:
            f.write("maybe_dga," + dga + "," + str(self.maybe_dga[dga]) + "\n")
        for tld in self.dga_tld:
            f.write("dga_tld," + tld + "," + str(self.dga_tld[tld]) + "\n")
        for dga in self.dga_sample:
            f.write("dga_sample," + dga + ",1\n")
        f.close()

    def import_result_file(self, result_file):
        #self.list = dict()
        for line in open(result_file , "rt", encoding="utf-8"):
            parts = line.split(",")
            if len(parts) != 3:
                print("Unexpected line in " + result_file + "\n" + line + "\ngiving up")
                exit(1)
            try:
                count = int(parts[2])
            except:
                print("Unexpected count in " + result_file + "\n" + line + "\ngiving up")
                exit(1)
            if parts[0] == "catsum":
                add_to_list(self.sum_by_cat, parts[1], count)
            elif parts[0] == "maybe_dga":
                add_to_list(self.maybe_dga, parts[1], count)
            elif parts[0] == "dga_tld":
                add_to_list(self.maybe_dga, parts[1], count)
            elif parts[0] == "dga_sample":
                self.dga_sample.append(parts[1])
            else:
                print("Unexpected table in " + result_file + "\n" + line + "\ngiving up")
                exit(1)
