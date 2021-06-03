#!/usr/bin/python
# coding=utf-8
#
# The module examines the subdomain list project, retrieve its history, and builds
# a record of commits and dates complemented by number of lines in the project.
import sys
import os
import logparse
import pubsuffix


# main program
if len(sys.argv) != 2:
    print("Usage: " + sys.argv[0] + " results.csv")
    exit(1)
results = sys.argv[1]
tmpdir = "tmp"
pubsubdata = "public_suffix_list.dat"
startdir = os.getcwd()

# initialize the resuls list from the result file
resfile = logparse.logfile()
if os.path.isfile(results):
    resfile.load_file(results)
else:
    resfile.write_file(results)

# clone the publicsuffix project from GitHub and get the current log
try:
    os.system("rm -rf " + tmpdir)
except:
    print("Could not remove " + tmpdir)
os.system("git clone https://github.com/publicsuffix/list "+tmpdir)
os.chdir(tmpdir)
os.system("git log --date=iso-strict --format=\"%H,%cd,\" > dlog.csv")

# build the list of required commits
lp = logparse.logfile()
lp.diff_file("dlog.csv", resfile)
print("Log includes " + str(len(lp.list)) + " items.");
lp.write_file("olog.csv")

# todo, checkout the commit, compute the size of the file
for h in lp.list:
    os.system("git checkout -q " + h)
    if os.path.isfile(pubsubdata):
        lp.list[h].count = pubsuffix.pub_suffix_count(pubsubdata)
    os.chdir("..")
    logparse.logfile.append_line(results,lp.list[h])
    os.chdir(tmpdir)
