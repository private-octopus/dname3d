#!/usr/bin/python
# coding=utf-8
#
# TODO, V2
# Build the ip2as.csv file from the data prepared by APNIC:
#   https://bgp.potaroo.net/as6447/bgptab.txt.gz
#   https://bgp.potaroo.net/v6/as6447/bgptab.txt.gz
#
# The files are compressed. To simplify programming, we save them in a
# a temporary folder, passed as an argument to the program. Then
# we can read the files using:
#        
#                   for line in gzip.open(file_path, 'rt'):
#                       self.add_line(line, ...)
#
# The decompressed file are lists of tab of format:
#   1.0.0.0/24 13335
#   1.0.4.0/22 38803
#   1.0.5.0/24 38803
#   1.0.16.0/24 2519
#
# Notice that some the ranges are overlapping.
# 
# When resolving an address, we want the following:
# 
#   - The network prefix must match the address. If no
#     prefix does that, then return AS 0
#   - The network prefix must be the best match. For example,
#     if matching 1.0.5.1 with example above, both
#     1.0.4.0/22 and 1.0.5.0/24 match, we should return 1.0.5.0/24
#
# We assume that the list is sorted, starting with the start
# address of the network.
#
# If possible, reduce the size of the table. For example, in
# the input above, 1.0.4.0/22 completely includes 1.0.5.0/24
# and produces the same result. There is no point in keeping
# the second entry for our purpose. (In BGP, it might be
# useful because the routes may be different.)
#
# We do this using an IP overlap structure:
#
#    IPnetwork, ASnumber, list of overlapped network.
#
# We want to keep the existing ip2as code format for now, so
# we transform the input into the format:
#
#   first-ip-in-range, last-ip-in-range, AS number
#
# We want to match both v4 and v6. We will start with only v4,
# then add logic in ip2as.py to also support a V6 table.

from base64 import standard_b64decode
import gzip
import sys
import traceback
import ipaddress
import ip2as

import sys
import traceback
import ipaddress
import urllib.request, urllib.error, urllib.parse
import gzip

class ip_tab_line:
    def __init__(self):
        self.version = 0
        self.net = ipaddress.IPv4Network("0.0.0.0/0")
        self.as_number = 0
        self.overlap = []

    def from_line(self,line):
        ret = False
        try:
            parts = line.split(" ")
            if len(parts) == 2:
                self.net = ipaddress.ip_network(parts[0])
                self.as_number = int(parts[1].strip())
                ret = True
            else:
                print("malformed IP net line: <" + line.strip() + ">")
        except Exception as e:
            print("Cannot parse <" + line.strip() + ">, exception:" + str(e))
        return ret

    # tests whether this network overlaps another network
    def subnet_of(self, other):
        ret = False
        if self.net.version == other.net.version:
            ret = self.net.subnet_of(other.net)
        return ret

    def insert_overlapping(self, other, in_order):
        is_direct = True
        overlaps = self
        range_start = 0
        if in_order:
            range_start = -1
        for sub_tab in self.overlap[range_start:]:
            if other.subnet_of(sub_tab):
                is_direct = False
                sub_tab.insert_overlapping(other, in_order)
                break
        if is_direct and not self.as_number == other.as_number:
            self.overlap.append(other)

    def find_asn(self, subnet):
        asn = self.as_number
        #
        # TODO: replace linear search by binary
        #
        try:
            for sub_tab in self.overlap:
                if subnet.subnet_of(sub_tab.net):
                    asn = sub_tab.find_asn(subnet)
                    break
        except Exception as e:
            print("Cannot access subnet " + str(sub_tab.net) + ", exception:" + str(e))
        return asn

    def add_ranges(self, ranges):
        a_next = self.net.network_address
        for sub_tab in self.overlap:
            b_first = sub_tab.net.network_address
            if b_first != a_next:
                # insert a pseudo range from a_next to just before b_first
                ranges.add(a_next, ip2as.address_before(b_first), self.as_number)
            a_last = sub_tab.add_ranges(ranges)
            a_next = ip2as.address_after(a_last)
        a_last = ip2as.address_last(self.net)
        if a_next != a_last:
             # insert a pseudo range to finish the overlap
             ranges.add(a_next, a_last, self.as_number)
        return a_last


    def format_ip2as(self, F, nb_lines):
        a_next = self.net.network_address
        for sub_tab in self.overlap:
            b_first = sub_tab.net.network_address
            if b_first != a_next:
                # insert a pseudo range from a_next to just before b_first
                b_before = ip2as.address_before(b_first)
                F.write(str(a_next) + ',' + str(b_first) + "," + str(self.as_number) + "\n")
                nb_lines += 1

            a_last, nb_lines = sub_tab.format_ip2as(F, nb_lines)
            a_next = ip2as.address_after(a_last)
        a_last = ip2as.address_last(self.net)
        if a_next != a_last:
             # insert a pseudo range to finish the overlap
             F.write(str(a_next) + ',' + str(a_last) + "," + str(self.as_number) + "\n")
             nb_lines += 1
        return a_last, nb_lines

class bgp_tab_parser:
    def __init__(self):
        self.v4_tab = ip_tab_line()
        self.v6_tab = ip_tab_line()
        self.v6_tab.net = ipaddress.IPv6Network("::/0")
        self.nb_parse_errors = 0

    def parse_file(file_name, root_tab):
        ret = False
        nb_lines = 0
        try:
            for line in gzip.open(file_name, 'rt'):
                nb_lines += 1
                tab = ip_tab_line()
                if tab.from_line(line):
                    root_tab.insert_overlapping(tab, True)
            ret = True
            print("Read " + str(nb_lines) + " lines from " + file_name)
        except Exception as e:
            print("Cannot load " + file_name + ", exception:" + str(e))
        return ret

    def load_url(url, file_name):
        ret = False 
        try:
            with open(file_name,"wb") as F:
                response = urllib.request.urlopen(url)
                file_data = response.read()
                data_length = len(file_data)
                print("Loaded " + str(data_length) + " from " + url)
                F.write(file_data)
                ret = True
        except Exception as e:
            print("Cannot load <" + url + "> in " + file_name + ", exception:" + str(e))
        return ret

    def parse_version(self, temp, version):
        if version == 4:
            file_name = temp + "bgp_tab_v4.txt.gz"
            url = "https://bgp.potaroo.net/as6447/bgptab.txt.gz"
            v_tab = self.v4_tab
        elif version == 6:
            file_name = temp + "bgp_tab_v6.txt.gz"
            url = "https://bgp.potaroo.net/as6447/bgptab.txt.gz"
            v_tab = self.v4_tab
        else:
            print("Unexpected version: " + str(version))
            return False
        ret = bgp_tab_parser.load_url(url, file_name)
        if ret:
            ret = bgp_tab_parser.parse_file(file_name, v_tab)

    



# Parsing program starts here

target_v4 = sys.argv[1]
temp = sys.argv[2]
#old_table = sys.argv[3]

parser = bgp_tab_parser()

parser.parse_version(temp, 4)

test_addresses = [ "0.0.0.0", "1.0.0.1", "1.0.4.1", "1.0.5.1", "1.0.128.1", "128.116.0.1", "223.255.253.255", "240.1.1.1" ]
test_asn = [ 0, 13335, 38803, 38803, 23969, 22697, 58519, 0 ]

verified = True
for i in range(0, len(test_addresses)):
    ta = test_addresses[i]
    subnet = ipaddress.IPv4Network(ta + "/32")
    asn = parser.v4_tab.find_asn(subnet)
    if asn != test_asn[i]:
        print(ta + " -> " + str(asn) + ", expected " + str(test_asn[i]))
        verified = False

if verified:
    print("Input verified, formatting " + target_v4)
    ranges = ip2as.ip2as_table()
    a_last = parser.v4_tab.add_ranges(ranges)
    print("Found " + str(len(ranges.table)) + " ranges, end with " + str(a_last) + ", " + str(ranges.nb_zero()) + " zeroes.")
    #old_ranges = ip2as.ip2as_table()
    #old_ranges.load(old_table)
    #print("Found " + str(len(old_ranges.table)) + " in old_table, " + str(old_ranges.nb_zero()) + " zeroes.")
    #ranges.merge(old_ranges)
    #print("After merge, " + str(len(ranges.table)) + " ranges, " + str(ranges.nb_zero()) + " zeroes.")
    ranges.collapse()
    print("Kept " + str(len(ranges.table)) + " ranges after collapse, " + str(ranges.nb_zero()) + " zeroes.")
    if ranges.save(target_v4):
        print("Saved " + str(len(ranges.table)) + " ranges in " + target_v4)

    for i in range(0, len(test_addresses)):
        ta = test_addresses[i]
        asn = ranges.get_as_number(ta)
        if asn != test_asn[i]:
            print(ta + " -> " + str(asn) + ", expected " + str(test_asn[i]))
            verified = False
    if verified:
        print("New table was verified.")



