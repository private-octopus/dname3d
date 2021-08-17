#!/usr/bin/python
# coding=utf-8
#
# Unit test of the suffix details module including hyperloglog
#
# The following tests are expected to pass:
#   py .\suffix_details_test.py ../tmp/details_out.csv ../data/suffix_test_ref.csv "-" ../data/suffix_test_names.csv
#   py .\suffix_summary_test.py ../tmp/details_out.csv ../data/suffix_test_2_ref.csv "-" ../data/suffix_test_names.csv ../data/suffix_test_names_2.csv
#   py .\suffix_summary_test.py ../tmp/details_out.csv ../data/suffix_test_list_ref.csv ../data/suffix_test_list.txt ../data/suffix_test_names.csv ../data/suffix_test_names_2.csv


import sys
import suffixes
import nameparse
import compare_file_test
import traceback
import time
import os

# Define a couple of test functions for loading the test files
def load_logline(suf_s, line):
    nl = nameparse.nameline()
    if nl.from_csv(line):
        if nl.name_type == "tld":
            suf_s.add_name(nl.name, nl.count, nl.ip)

def load_logfile_csv(suf_s, logfile):
    for line in open(logfile , "rt"):
        load_logline(suf_s, line)

def load_logfile_gz(suf_s, logfile):
    try:
        with gzip.open(logfile,'rt') as fin: 
            for line in fin:
                load_logline(suf_s, line)
    except Exception as e:
        traceback.print_exc()
        print("Cannot process compressed file <" + logfile  + ">\nException: " + str(e))
        print("Giving up");
        exit(1) 
        
def load_logfile(suf_s, logfile):
    if logfile.endswith(".gz"):
        load_logfile_gz(suf_s, logfile)
    else:
        load_logfile_csv(suf_s, logfile)

# main program

suffix_out = sys.argv[1]
suffix_ref = sys.argv[2]
suffix_list = sys.argv[3]
files = sys.argv[4:]

sub_s = suffixes.suffix_details_file(4,3)

if suffix_list != "-":
    s_l = []
    for line in open(suffix_list , "rt", encoding="utf-8"):
        s_l.append(line.strip())
    sub_s.init_suffixes(s_l)

for file_name in files:
     load_logfile(sub_s, file_name)

sub_s.evaluate()

sub_s.save(suffix_out)

if not compare_file_test.compare_files(suffix_out, suffix_ref):
    exit(1)
else:
    exit(0)


