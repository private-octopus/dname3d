#!/usr/bin/python
# coding=utf-8
#
# Unit test of the namestats module

import sys
import namestats
import compare_file_test

# main program

result_out = sys.argv[1]
result_ref = sys.argv[2]
suffix_out = sys.argv[3]
suffix_ref = sys.argv[4]
dga_subnets_file = sys.argv[5]
input_files = sys.argv[6:]

if len(input_files) == 0:
    print("No files to process!")
    exit(1)

if dga_subnets_file != "-":
    dga_subnets = namestats.subnet_dict_from_file(dga_subnets_file)
else:
    dga_subnets = dict()

stats = namestats.namestats(dga_subnets)
for input_file in input_files:
    stats.load_logfile(input_file)
stats.final_dga()
# Perform sort and evaluation before saving the output, 
# so results are easier to compare
stats.export_result_file(result_out, do_sort=True)
stats.suffixes.top_by_subs(suffix_out)
# Compare outouts to expected result
if not compare_file_test.compare_files(result_out, result_ref) or \
   not compare_file_test.compare_files(suffix_out, suffix_ref):
    exit(1)
else:
    exit(0)
