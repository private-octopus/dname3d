#!/usr/bin/python
# coding=utf-8
#
# Extract name statistics from a set of files.
# Since the number of files to merge can be quite large, we spread the processing
# on all available cores.


import sys
import prefixlist
import traceback
import time
import concurrent.futures
import os

def usage(argv_0):
    print("Usage:\n" + argv_0 + " top_suffix_file nb_saved suffix_file*")
    print("    top_suffix_file:  file in which top N suffixes will be saved.")
    print("    nb_saved: number of suffixes to retain (0 means all).")
    print("    suffix_file*:  file in which suffixes were collected.")

# main loop
def main():
    if len(sys.argv) < 4:
        usage(sys.argv[0])
        exit(1)
    nb_saved = int(sys.argv[2])

    pss = prefixlist.suffix_summary_sorter(4, nb_saved)
    for suffix_file in sys.argv[3:]:
        pss.parse_suffix_summary(suffix_file)
        sys.stdout.write(".")
        sys.stdout.flush()
    sys.stdout.write("\n")
    pss.save_summary(sys.argv[1])

# actual main program, can be called by threads, etc.

if __name__ == '__main__':
    main()