#!/usr/bin/python
# coding=utf-8
#
# Extract name statistics from a set of files.
# Since the number of files to merge can be quite large, we spread the processing
# on all available cores.


import sys
import namestats
import traceback
import time
import concurrent.futures
import os

def usage(argv_0):
    print("Usage:\n" + argv_0 + " merged_suffix_file nb_saved suffix_file*")
    print("    merged_suffix_file:  file in which all suffixes will be merged.")
    print("    nb_saved: number of suffixes to retain (0 means all).")
    print("    suffix_file*:  file in which suffixes were collected.")

# main loop
def main():
    if len(sys.argv) < 4:
        usage(sys.argv[0])
        exit(1)
    nb_saved = int(sys.argv[2])

    sublist = dict()
    nst = namestats.namestats(sublist)
    for suffix_file in sys.argv[3:]:
        nst.import_suffix_file(suffix_file)
    nst.suffixes.save_suffix_summary(sys.argv[1],sort=True, eval=True, top_n=nb_saved)

# actual main program, can be called by threads, etc.

if __name__ == '__main__':
    main()