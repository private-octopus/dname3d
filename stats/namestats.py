#!/usr/bin/python
# coding=utf-8
#
# Parse the names file and extract statistics similar to sum3,
# but also including the dga13 category.

import traceback
import nameparse
import ipaddress
import suffixes
import gzip
import traceback

def subnet_dict_from_file(file_name):
    subdict = dict()
    for line in open(file_name , "rt", encoding="utf-8"):
        subdict[line.strip()] = 0
    return subdict

def ip_in_subnet_dict(d, ip):
    ipa = ipaddress.ip_address(ip)
    if ipa.version == 4:
        isn = ipaddress.ip_network(ip + "/24", strict=False)
    elif ipa.version == 6:
        isn = ipaddress.ip_network(ip + "/48", strict=False)
    else:
        isn = ipaddress.ip_network("::/64")
    isnt = str(isn)
    return (isnt in d)

def add_to_list(table, name, count):
    if name in table:
        table[name] += count
    else:
        table[name] = count

class dga13maybe:
    def __init__(self):
        self.ip = ""
        self.count = 0

class namestats:
    def __init__(self, sublist):
        #self.list = dict()
        self.sum_by_cat = dict()
        self.maybe_dga = dict()
        self.dga_count = 0
        self.suffixes = suffixes.suffix_summary_file(4, 3)
        self.sublist = sublist
        self.p0_count = []
        for i in range(0,64):
            self.p0_count.append(0)

    def ip_is_in_sublist(self, ip):
        return ip_in_subnet_dict(self.sublist, ip)

    def final_dga(self):
        if "tld" in self.sum_by_cat:
            has_dga13 =  self.sum_by_cat["tld"] < 10*self.dga_count
        else:
            has_dga13 = False
        for name in self.maybe_dga:
            ip = self.maybe_dga[name]
            if ip != "-":
                if has_dga13 or (ip != '' and self.ip_is_in_sublist(ip)):
                    add_to_list(self.sum_by_cat, "dga13", 1)
                else:
                    add_to_list(self.sum_by_cat, "tld", 1)
                    self.suffixes.add_name(name, 1)
        self.maybe_dga = dict()

    def trial_dga(self, name, ip, count):
        # once identified as multiple count, we retain the name in the 
        # list but mark the IP address as special, indicating it
        # was already processed.
        if name in self.maybe_dga:
            if self.maybe_dga[name] != "-":
                if self.dga_count > 0:
                    self.dga_count -= 1
                count += 1
                self.maybe_dga[name] = "-"
            if count > 0:
                add_to_list(self.sum_by_cat, "tld", count)
        else:
            self.maybe_dga[name] = ip
            if ip != "-":
                self.dga_count += 1

    def load_name(self, name, count, ip):
        parts = name.split(".")
        nb_parts = len(parts)
        # remove final dot if any
        while nb_parts > 0 and len(parts[nb_parts-1]) == 0:
            nb_parts -= 1
        # TODO: trim to specified depth
        if nb_parts > 0 and parts[nb_parts -1] == "ARPA":
            add_to_list(self.sum_by_cat, "arpa", count)
        else:
            if nb_parts > 0:
                lp0 = len(parts[0])
                if lp0 > 63:
                    lp0 = 63
            else:
                lp0 = 0
            self.p0_count[lp0] += 1

            if nb_parts >= 2 and count == 1 and (len(parts[0]) == 12 or len(parts[0]) == 13):
                self.trial_dga(name, ip, 1)
            else:
                if nb_parts > 2:
                    self.trial_dga(parts[-2] + "." + parts[-1], "-", 0)
                add_to_list(self.sum_by_cat, "tld", count)
                self.suffixes.add_name(name, count)

    def load_logline(self, line):
        nl = nameparse.nameline()
        if nl.from_csv(line):
            if nl.name_type == "tld":
                self.load_name(nl.name, nl.count, nl.ip)
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

    def export_result_table(f, table_name, table, do_sort=False):
        key_list = list(table.keys())
        if do_sort:
            key_list = sorted(key_list)
        for key in key_list:
            f.write(table_name + "," + key + "," + str(table[key]) + "\n")

    def export_result_file(self, result_file, do_sort=False):
        #self.list = dict()
        f = open(result_file , "wt", encoding="utf-8")
        namestats.export_result_table(f, "catsum" , self.sum_by_cat, do_sort)
        namestats.export_result_table(f, "maybe_dga" , self.maybe_dga, do_sort)
        for i in range(0,64):
            f.write("p0_count," + str(i) + "," + str(self.p0_count[i]) + "\n")
        f.close()

    def import_result_file(self, result_file):
        #self.list = dict()
        for line in open(result_file , "rt", encoding="utf-8"):
            count = 0
            ip = "-"
            parts = line.split(",")
            if len(parts) != 3:
                print("Unexpected line in " + result_file + "\n" + line + "\ngiving up")
                exit(1)
            try:
                if parts[0] == "maybe_dga":
                    ip = parts[2].strip()
                    if ip != "-":
                        count = 1
                else:
                    count = int(parts[2])
            except:
                print("Unexpected count in " + result_file + "\n" + line + "\ngiving up")
                exit(1)
            if parts[0] == "catsum":
                add_to_list(self.sum_by_cat, parts[1], count)
            elif parts[0] == "maybe_dga":
                self.trial_dga(parts[1], ip, count)
            elif parts[0] == "p0_count":
                self.p0_count[int(parts[1].strip())] += count
            else:
                print("Unexpected table in " + result_file + "\n" + line + "\ngiving up")
                exit(1)
                
    def export_suffix_file(self, suffix_file, need_sort=False):
        self.suffixes.prune(0,1)
        self.suffixes.save_suffix_summary(suffix_file, sort=need_sort)

    def import_suffix_file(self, suffix_file):
        self.suffixes.parse_suffix_summary(suffix_file)