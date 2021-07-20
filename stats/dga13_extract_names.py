#!/usr/bin/python
# coding=utf-8
#
# Extract the list of DGA13 prefixes from file names
#
# Take as input a list of IP addresses and a list of name files.
# Extract only the names that are likely DGA13 and the IP address matches.
# Accumulates a list of names.

import namestats
import traceback
import nameparse
import ipaddress
import prefixlist
import gzip
import sys
import time
import os
import parallel_buckets

class dga13_bucket_param:
    def __init__(self, dga_subnets, temp_prefix, suffix_name):
        self.dga_subnets = dga_subnets
        self.temp_prefix = temp_prefix
        self.suffix_name = suffix_name

class dga13_bucket:
    def __init__(self):
        self.bucket_id = 0
        self.input_files = []
        self.dga_subnets = dict()
        self.suffix_file_name = ""
        self.p0_count_file_name = ""
        self.suffixes = dict()
        self.p0_count = []
        for i in range(0,64):
            self.p0_count.append(0)

    def complete_init(self, param):
        self.dga_subnets = param.dga_subnets
        self.suffix_file_name = param.temp_prefix + str(self.bucket_id) + param.suffix_name
        self.p0_count_file_name = param.temp_prefix + str(self.bucket_id) + "_p0count.csv"

    def load_logline(self, line):
        nl = nameparse.nameline()
        try:
            if nl.from_csv(line):
                if nl.name_type == "tld":
                    parts = nl.name.split(".")
                    if len(parts) > 1 and \
                        nl.count == 1 and \
                        parts[-1] != "ARPA" and \
                        (nl.ip != '' and namestats.ip_in_subnet_dict(self.dga_subnets, nl.ip)):
                        lp0 = len(parts[0])
                        if lp0 > 63:
                            lp0 = 63
                        self.p0_count[lp0] += 1
                        if lp0 == 12 or lp0 == 13:
                            suffix = parts[1]
                            for part in parts[2:]:
                                suffix += "." + part 
                            if suffix in self.suffixes:
                                self.suffixes[suffix] += 1
                            else:
                                self.suffixes[suffix] = 1                 
        except Exception as e:
            traceback.print_exc()
            print("Cannot process input line:" + line  + "\nException: " + str(e))
        
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

    def load(self):
        try:
            for input_file in self.input_files:
                self.load_logfile(input_file)
        except:
            traceback.print_exc()
            print("Abandon bucket " + str(self.bucket_id))
        return True

    def save(self):
        keys = sorted(self.suffixes.keys())
        with open(self.suffix_file_name , "wt", encoding="utf-8") as f:
            for key in keys:
                f.write(key + "," + str(self.suffixes[key]) + "\n")
        
        with open(self.p0_count_file_name , "wt", encoding="utf-8") as f:
            for i in range(0,64):
                f.write(str(i) + "," + str(self.p0_count[i]) + "\n")

    def import_list(self, suffix_file_name):
        try:
            for line in open(suffix_file_name , "rt", encoding="utf-8"):
                try:
                    p = line.split(",")
                    s = p[0].strip()
                    c = int(p[1].strip())
                    if s in self.suffixes:
                        self.suffixes[s] += c
                    else:
                        self.suffixes[s] = c
                except Exception as e:
                    traceback.print_exc()
                    print("In file <" + suffix_file_name  + ">\nCannot parse line:\n" + line.strip() + "\nException: " + str(e))
                    exit(1)
        except Exception as e:
            traceback.print_exc()
            print("For file <" + suffix_file_name  + ">\nException: " + str(e))
            exit(1)

    
    def import_p0_count(self, p0_count_file_name):
        try:
            for line in open(p0_count_file_name , "rt", encoding="utf-8"):
                try:
                    p = line.split(",")
                    i = int(p[0].strip())
                    c = int(p[1].strip())
                    if i >= 0 and i < 64:
                        self.p0_count[i] += c
                    else:
                        print("Unexpected count: " + line.strip())
                except Exception as e:
                    traceback.print_exc()
                    print("In file <" + p0_count_file_name  + ">\nCannot parse line:\n" + line.strip() + "\nException: " + str(e))
                    exit(1)
            
        except Exception as e:
            traceback.print_exc()
            print("For file <" + suffix_file_name  + ">\nException: " + str(e))
            exit(1)

def usage(argv0):
    print("Usage: " + argv0 + " suffix_file_name tmp_prefix dga_subnet_file name_files*")
    exit(1)

# main loop
def main():
    if len(sys.argv) < 5:
        usage(sys.argv[0])
        exit(1)

    suffix_file_name = sys.argv[1]
    p0_count_file_name = sys.argv[2]
    temp_prefix = sys.argv[3]
    dga_subnets_file = sys.argv[4]
    files = sys.argv[5:len(sys.argv)]

    if dga_subnets_file != "-":
        dga_subnets = namestats.subnet_dict_from_file(dga_subnets_file)
    else:
        dga_subnets = dict()

    params = dga13_bucket_param(dga_subnets, temp_prefix, "_dga13_sfx.csv")
    if temp_prefix == "-":
        nb_process = 1
    else:
        nb_process = os.cpu_count()
    print("Aiming for " + str(nb_process) + " processes")

    # prepare a set of buckets for as many processes.
    bucket_list = parallel_buckets.init_buckets(dga13_bucket, nb_process, files, params)

    print("Will use " + str(len(bucket_list)) + " buckets")
    
    start_time = time.time()
    if len(bucket_list) > 1:
        parallel_buckets.run_buckets(bucket_list)
        bucket_time = time.time()
        print("\nThreads took " + str(bucket_time - start_time))
        summary = dga13_bucket()
        summary.dga_subnets = dga_subnets
        summary.suffix_file_name = suffix_file_name
        summary.p0_count_file_name = p0_count_file_name
        for bucket in bucket_list:
            summary.import_list(bucket.suffix_file_name)
            summary.import_p0_count(bucket.p0_count_file_name)
            sys.stdout.write(".")
            sys.stdout.flush()
        summary_time = time.time()
        print("\nSummary took " + str(summary_time - bucket_time))
        summary.save()
        save_time = time.time()
        print("Loaded " + str(len(bucket_list)) + " buckets into " + suffix_file_name)
        print("\nSave took " + str(save_time - summary_time))
    else:
        bucket_list[0].suffix_file_name = suffix_file_name
        bucket_list[0].p0_count_file_name = p0_count_file_name
        bucket_list[0].load()
        load_time = time.time()
        print("\nLoad took " + str(load_time - start_time))
        bucket_list[0].save()
        save_time = time.time()
        print("Loaded a single bucket into " + suffix_file_name)
        print("\nSave took " + str(save_time - load_time))

# actual main program, can be called by threads, etc.

if __name__ == '__main__':
    main()

