#!/usr/bin/python
# coding=utf-8
#
# Parse the names file and extract statistics similar to sum3,
# but also including the dga13 category.

import traceback
import sys
#!/usr/bin/python
# coding=utf-8
#
# Merge a number of prefix entries into a single prefix summary.


import sys
import prefixlist
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
            suf_s.add_name(nl.name, nl.count)

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
files = sys.argv[3:]

sub_s = prefixlist.suffix_summary_file(4,3)

for file_name in files:
     load_logfile(sub_s, file_name)

sub_s.evaluate()

sub_s.top_by_subs(suffix_out, 0)

if not compare_file_test.compare_files(suffix_out, suffix_ref):
    exit(1)
else:
    exit(0)


