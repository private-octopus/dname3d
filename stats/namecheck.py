#!/usr/bin/python
# coding=utf-8
#
# Merging of name files into a single list and extraction of the common prefixes
import sys
import prefixlist

def usage(argv_0):
    print("Usage:\n" + argv_0 + " depth prefix_file name_file*")
    print("    depth:        maximum prefix length to be tabulated.")
    print("    prefix_file:  file in which prefixes will be collected.")
    print("    name_file*:   at least one file containing name lists.")

# Main program
depth = 0
if len(sys.argv) >= 4:
    try:
        depth = int(sys.argv[1])
    except:
        depth = 0
    if depth == 0:
        print("Incorrect depth value: " + argv[1])
if depth == 0:
    usage(sys.argv[0])
    exit(1)

prefix_list = prefixlist.prefixlist(depth)
result_file = sys.argv[2]

for file_name in sys.argv[3:len(sys.argv)]:
    print("loading: " + file_name)
    prefix_list.load_logfile(file_name)
    print("There are now " + str(len(prefix_list.list)) + " prefixes")

prefix_list.write_file(result_file)