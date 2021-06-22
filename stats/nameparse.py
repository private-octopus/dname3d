#!/usr/bin/python
# coding=utf-8
#
# Parse the names file.
# Accumulate all names in a single file that can be saved.

import traceback

class nameline:
    def __init__(self):
        self.name = ""
        self.is_nx = 0
        self.name_type = ""
        self.count = 0
        self.ip = ""

    def copy(self):
        cp = nameline()
        cp.name = self.name
        cp.is_nx = self.is_nx
        cp.name_type = self.name_type
        cp.count = self.count
        cp.ip = self.ip
        return cp

    def from_csv(self, line):
        ret = False
        p = line.split(",")
        if len(p) >= 4 and len(p) <= 5:
            self.name = p[0]
            try:
                self.is_nx = int(p[1])
            except:
                self.is_nx = 0
            self.name_type = p[2]
            try:
                self.count = int(p[3])
                if self.count > 0:
                    ret = True
            except:
                self.count = 0
            if len(p) == 5:
                self.ip = p[4]
        return ret

    def to_csv(self):
        t = self.name + "," + str(self.is_nx) + "," + self.name_type + "," + str(self.count) + "," + self.ip + "\n"
        return t

    def csv_head():
        return("name, is_nx, name_type, count, ip\n")

class namelist:
    def __init__(self):
        self.list = dict()

    def add_entry(self, nl):
        if nl.name in self.list:
            self.list[nl.name].count += nl.count
            if self.list[nl.name].ip == "":
                self.list[nl.name].ip = nl.ip
        else:
            self.list[nl.name] = nl

    def add_line(self, line):
        nl = nameline()
        if nl.from_csv(line):
            add_entry(nl)

    def load_file(self, logfile):
        for line in open(logfile , "rt"):
            self.add_line(line)

    def write_file(self,logfile):
        f = open(logfile , "wt", encoding="utf-8")
        f.write(nameline.csv_head())
        for name in self.list:
            f.write(self.list[name].to_csv())
        f.close()



