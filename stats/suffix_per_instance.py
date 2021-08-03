#!/usr/bin/python
# coding=utf-8
#
# Take a list of tracked suffixes and a list of per instance statistics
# Build a list of <instance><suffix><count>
# Retain for each listed suffix the N instances in which the suffix appears

import sys
import ipaddress
import traceback
import time
import concurrent.futures
import os
import prefixlist

# main loop
instance_suffix_file = sys.argv[1]
tracked_suffix_file = sys.argv[2]
nb_suffixes = 0
if sys.argv[3] != "-":
    nb_suffixes = int(sys.argv[3])
instance_files = sys.argv[4:]
tracked_suffix = []

for line in open(tracked_suffix_file , "rt"):
    # assume first argument in csv file
    p = line.split(",")
    tracked_suffix.append(p[0].strip())
    if nb_suffixes > 0 and len(tracked_suffix) > nb_suffixes:
        break

f = open(instance_suffix_file , "wt", encoding="utf-8")
for isf in instance_files:
    sf = prefixlist.suffix_summary_file(4, 3)
    sf.parse_suffix_summary(isf)
    for suffix in tracked_suffix:
        if sfi in sf.summary:
            f.write(isf + "," + sfi + "," + str(sf.summary[sfi].subs) + "," + str(sf.summary[sfi].hits))
f.close()


