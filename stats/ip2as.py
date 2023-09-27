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

# TODO, V2
# We start from a bgp "tab" list, of format:
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
# If possible, reduce the size of the table. For example, in
# the input above, 1.0.4.0/22 completely includes 1.0.5.0/24
# and produces the same result. There is no point in keeping
# the second entry for our purpose. (In BGP, it might be
# useful because the routes may be different.)
#
# The V1 format assumes non overlapping lines, but this
# increases the size of the table. Is that the right choice?
#
# We want to match both v4 and v6. Maybe have two tables in
# a single object? Or rely on an ordering function?
#
#
# 
# 
#  
#                if input_file.file_name.endswith(".gz"):      
#                   for line in gzip.open(file_path, 'rt'):
#                       self.add_line(line, input_file.slice)
#               else:    
#                   for line in open(file_path):
#                       self.add_line(line, input_file.slice)

import gzip
import sys
import traceback
import ipaddress


def address_last(sn):
    ar_last = bytearray(sn.network_address.packed)
    ar_mask = bytearray(sn.hostmask.packed)
    for i in range(0, len(ar_last)):
        ar_last[i] |= ar_mask[i]
    ip_last = ipaddress.ip_address(bytes(ar_last))
    return ip_last

def address_after(addr):
    ar_next = bytearray(addr.packed)
    i = len(ar_next) - 1
    reminder = 1
    while i >= 0 and reminder != 0:
        x = int(ar_next[i]) + 1
        if x > 255:
            ar_next[i] = 0
        else:
            ar_next[i] = x
            reminder = 0
        i -= 1
    a_next = ipaddress.ip_address(bytes(ar_next))
    return a_next

def address_before(addr):
    ar_before = bytearray(addr.packed)
    i = len(ar_before) - 1
    reminder = 1
    while i >= 0 and reminder != 0:
        x = int(ar_before[i])
        if x > 0:
            ar_before[i] = x - 1
            reminder = 0
        else:
            ar_before[i] = 255
        i -= 1
    a_before = ipaddress.ip_address(bytes(ar_before))
    return a_before

class ip2as_line:
    def __init__(self, ip_first, ip_last, as_number):
        self.ip_first = ip_first
        self.ip_last = ip_last
        self.as_number = as_number

    def load(self, s):
        ret = True
        st = s.strip()
        parts = st.split(",")
        is_first = True
        try:
            self.ip_first = ipaddress.ip_address(parts[0].strip())
            self.ip_last = ipaddress.ip_address(parts[1].strip())
            self.as_number = int(parts[2].strip())
        except Exception as e:
            if is_first:
                # silently skip header  line of table
                is_first = False
            else:
                traceback.print_exc()
                print("For <" + st + ">: " + str(e))
            ret = False
        return(ret)

class ip2as_table:
    def __init__(self, ipv=4):
        self.table = []
        self.ip_version = ipv

    def load(self,file_name):
        ret = True
        try:
            first = True
            for line in open(file_name, "rt"):
                l = line.strip()
                if first and l == "ip_first, ip_last, as_number,":
                    first = False
                    continue
                if self.ip_version == 4:
                    il = ip2as_line(ipaddress.ip_address("0.0.0.0"), ipaddress.ip_address("0.0.0.0"), 0)
                else:
                    il = ip2as_line(ipaddress.ip_address("::0"), ipaddress.ip_address("::0"), 0)
                if il.load(l):
                    self.table.append(il)
        except Exception as e:
            traceback.print_exc()
            print("When loading <" + file_name + ">: " + str(e))
            ret = False
        return ret

    def save(self, file_name):
        ret = False
        try:
            with open(file_name, "wt") as F:
                F.write("ip_first, ip_last, as_number\n")
                for r in self.table:
                    F.write(str(r.ip_first) + ',' + str(r.ip_last) + "," + str(r.as_number) + "\n")
                ret = True
        except Exception as e:
            print("Cannot save ranges in " + file_name + ", exception:" + str(e))
        return ret

    def add(self, ip_first, ip_last, as_number):
        r = ip2as_line(ip_first, ip_last, as_number)
        self.table.append(r)

    def collapse(self):
        if len(self.table) < 2:
            print("Cannot collapse table of length " + str(len(self.table)))
        else:
            new_table = []
            current_range = self.table[0]
            for r in self.table[1:]:
                if r.as_number == current_range.as_number:
                    current_range.ip_last = r.ip_last
                else:
                    new_table.append(current_range)
                    current_range = r
            new_table.append(current_range)
            self.table = new_table

    def merge(self, other):
        new_table = []
        i_self = 0
        i_other = 0
        l_self = len(self.table)
        l_other = len(other.table)

        while i_self < l_self:
            if self.table[i_self].as_number != 0 or i_other >= l_other:
                new_table.append(self.table[i_self])
            else:
                ip_first = self.table[i_self].ip_first
                ip_last = self.table[i_self].ip_last
                while ip_first < ip_last and i_other < l_other:
                    if other.table[i_other].ip_last < ip_first:
                        # skip the other ranges that do not overlap this one
                        i_other += 1
                    elif other.table[i_other].ip_first <= ip_first:
                        # the other range starts before this one
                        ar = ip2as_line(ip_first, ip_last, other.table[i_other].as_number)
                        if other.table[i_other].ip_last < ip_last:
                            ar.ip_last = other.table[i_other].ip_last
                        ip_first = address_after(ar.ip_last)
                        new_table.append(ar)
                    else:
                        # no overlap at the beginning of the range
                        ar = ip2as_line(ip_first, ip_last, self.table[i_self].as_number)
                        if other.table[i_other].ip_first < ip_last:
                            ar.ip_last = address_before(other.table[i_other].ip_first)
                        ip_first = address_after(ar.ip_last)
                        new_table.append(ar)
                if ip_first < ip_last:
                    # some range left after the last entry in the other file
                    ar = ip2as_line(ip_first, ip_last, self.table[i_self].as_number)
                    new_table.append(ar)
            i_self += 1
        self.table = new_table

    def nb_zero(self):
        n = 0
        for range in self.table:
            if range.as_number == 0:
                n += 1
        return n
    
    def get_as_number(self, s):
        as_number = 0
        i_first = 0
        i_last = len(self.table) - 1
        try:
            addr = ipaddress.ip_address(s)
            if addr >= self.table[i_first].ip_first:
                if addr >= self.table[i_last].ip_first:
                    i_first = i_last
                else:
                    while i_first + 1 < i_last:
                        i_med = int((i_first + i_last)/2)
                        if addr >= self.table[i_med].ip_first:
                            i_first = i_med
                        else:
                            i_last = i_med
                if addr <= self.table[i_first].ip_last:
                    as_number = self.table[i_first].as_number
        except Exception as e:
            traceback.print_exc()
            print("When evaluating <" + s + ">: " + str(e))
            pass
        return as_number

class asname:
    def __init__(self):
        self.table = dict()
        self.aggregate = dict()
        for pair in [
            [ 9999999, "AKAMAI (multiple Ases)", "ZZ" ],
            [ 9999998, "AMAZON & AWS (multiple Ases)", "ZZ" ],
            [ 9999997, "Cloudflare (multiple Ases)", "ZZ" ],
            [ 9999996, "Google (multiple Ases)", "US" ],
            [ 9999995, "MICROSOFT (multiple Ases)", "US" ],
            [ 9999994, "OVH (multiple Ases)", "FR" ] ]:
            self.aggregate[pair[0]] = pair

    def load(self, file_name, test=False):
        ret = True
        nb_asn = 0
        try:
            for line in open(file_name, "rt"):
                l = line.strip()
                asn = 0
                as_check = l[0:2]
                asn_x = l[2:14].strip()
                name = l[14:]
                if test and nb_asn < 10:
                    print(as_check + "//" + asn_x + "//" + name)
                try:
                    asn = int(asn_x)
                    if not asn in self.table:
                        self.table[asn] = name
                        nb_asn += 1
                    elif test:
                        print("Duplicate: " + str(asn) + ", \"" + name + "\" (\"" + self.table[asn] + "\")")
                except Exception as e:
                    traceback.print_exc()
                    print("When parsing asn \"" + asn_x + "\": " + str(e))
                    ret = False
                    break
        except Exception as e:
            traceback.print_exc()
            print("When loading <" + file_name + ">: " + str(e))
            ret = False
        return ret

    def name(self, asn):
        n = "?"
        if asn in self.aggregate:
            n = self.aggregate[asn][1]
        if asn in self.table:
            n = self.table[asn]
        return n

class aggregated_asn:
    def __init__(self):
        self.aggregate = dict()
        for pair in [
            [ 33905, 9999999],
            [ 16625, 9999999],
            [ 20940, 9999999],
            [ 21342, 9999999],
            [ 8987, 9999998],
            [ 16509, 9999998],
            [ 14618, 9999998],
            [ 44298, 9999998],
            [ 13335, 9999997],
            [ 209242, 9999997],
            [ 15169, 9999996],
            [ 19527, 9999996],
            [ 16591, 9999996],
            [ 396982, 9999996],
            [ 3598, 9999995],
            [ 8068, 9999995],
            [ 8069, 9999995],
            [ 8070, 9999995],
            [ 8075, 9999995],
            [ 16276, 9999994],
            [ 35540, 9999994]]:
            self.aggregate[pair[0]] = pair[1]

    def get_asn(self, asn):
        if asn in self.aggregate:
            asn = self.aggregate[asn]
        return asn


def load_ip2as(ip2as_file):
    i2a = ip2as_table()
    if i2a.load(ip2as_file):
        print("From <" + ip2as_file + ">, loaded table of length: " + str(len(i2a.table)))
    else:
        print("Could not load <" + ip2as_file + ">")
        exit(1)
    return i2a