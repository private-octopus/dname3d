#!/usr/bin/python
# coding=utf-8
#
# IP Address to AS number conversion tool.
# The input is the file produced by "ip2asbuilder.py". This is a csv file,
# with three columns:
#    ip_first: first IP address in range
#    ip_last: last address IP address in range
#    as_number: autonomous system number for that range
# As of August 2020, that file has over 300000 entries, so memory size is
# a concern. We write an adhoc parser for that file, with three elements
# per line:
#    ip_first
#    ip_last
#    AS number.
# The table is ordered, and the ranges do not overlap. We use that to
# implement a simple binary search algorithm, finding the highest index in
# the table such that beginning <= search address. We then return the
# associated AS number if the address is in range, or 0 if it is not.

import sys
import traceback
import ipaddress
import ip2as

if len(sys.argv) != 3:
    print("Usage: " + sys.argv[0] + " ip2as.csv asnames.txt")
    exit(1)

ip2as_file = sys.argv[1]
asname_file = sys.argv[2]

t = ip2as.ip2as_table()

if t.load(ip2as_file):
    print("Loaded ip2as table of length: " + str(len(t.table)))
else:
    print("Could not load \"" + ip2as_file + "\"")

test_addr = ["0.0.0.0", "0.0.0.255", "1.0.0.0", "1.0.0.63", "1.0.0.255", "1.0.1.0",
              "123.45.67.8", "123.48.7.6", "223.255.253.255", "224.0.0.0" ]

test_asn = [0, 0, 13335, 13335, 13335, 0, 0, 18126, 58519, 0]

i = 0
ret = 0
while i < len(test_addr):
    asn = t.get_asn(test_addr[i])
    print(test_addr[i] + " --> " + str(asn))
    if asn != test_asn[i]:
        print("Expected: " + str(test_asn[i]))
        ret = 1
    i += 1

ndb = ip2as.asname()

if ndb.load(asname_file, test=True):
    print("Loaded asname table of length: " + str(len(ndb.table)))
else:
    print("Could not load \"" + asname_file  + "\"")

test_named_as = [0, 1, 18126, 58519, 4200001179, 9999999999 ]
test_named_as_name = ["?", "LVLT-1, US", "CTCX Chubu Telecommunications Company, Inc., JP", \
    "CHINATELECOM-CTCLOUD Cloud Computing Corporation, CN", "--No Registry Entry--", "?" ]

i = 0
while i < len(test_named_as):
    asn_t = ndb.name(test_named_as[i])
    print(str(test_named_as[i]) + " --> \"" + asn_t +"\"")
    if asn_t != test_named_as_name[i]:
        print("Expected: \"" + test_named_as_name[i] + "\"")
        ret = 1
    i += 1

if ret == 0:
    print("All tests pass.")

exit (ret)