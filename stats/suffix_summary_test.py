#!/usr/bin/python
# coding=utf-8
#
# Unit test of the suffix summary module including hyperloglog


import sys
import suffixes
import nameparse
import compare_file_test
import traceback
import time
import os
# Test of the summarization of detail result files.
#
# The following tests are expected to work:
#
# py .\suffix_summary_test.py ../tmp/details_out.csv ..\data\suffix_test_ref.csv ../data/suffix_test_ref.csv
# py .\suffix_summary_test.py ../tmp/details_out.csv ..\data\suffix_test_2_ref.csv ../data/suffix_test_ref.csv ..\data\suffix_test_details_2.csv

# main program

suffix_out = sys.argv[1]
suffix_ref = sys.argv[2]
files = sys.argv[3:]

sub_s = suffixes.suffix_details_file(4,3)

for file_name in files:
    sub_s.parse(file_name)

sub_s.evaluate()

sub_s.save(suffix_out)

if not compare_file_test.compare_files(suffix_out, suffix_ref):
    exit(1)
else:
    exit(0)


