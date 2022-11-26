#!/usr/bin/python
# coding=utf-8
#
# Compare the IP addresses and usage count found in two files. 
# Only retain the IP addresses that show more than 100 hits.

import sys
import ipaddress

class dgaByIp:
    def __init__(self):
        self.ip = ""
        self.count = 0

    def from_text(self, line):
        parts = line.split("\t")
        try:
            count = int(parts[1])
            ip_address = ipaddress.ip_address(parts[0])
            if count >= 100:
                self.ip = str(ip_address)
                self.count = count
                return True
        except:
            print("Not a correct entry: " + line)
        return False

    def table_from_file(f_name):
        t = []
        for line in open(f_name,"r"):
            dip = dgaByIp()
            if dip.from_text(line):
                t.append(dip)
        return t

    def myComp(self, other):
        if (self.ip < other.ip):
            return -1
        elif (self.ip > other.ip):
            return 1
        else:
            return 0
    
    def __lt__(self, other):
        return self.myComp(other) < 0
    def __gt__(self, other):
        return self.myComp(other) > 0
    def __eq__(self, other):
        return self.myComp(other) == 0
    def __le__(self, other):
        return self.myComp(other) <= 0
    def __ge__(self, other):
        return self.myComp(other) >= 0
    def __ne__(self, other):
        return self.myComp(other) != 0

class dgaBySubnet:
    def __init__(self):
        self.sub = ""
        self.total = 0
        self.count1 = 0
        self.count2 = 0

    def subnet_from_ip(ip):
        ipa = ipaddress.ip_address(ip)
        if ipa.version == 4:
            isn = ipaddress.ip_network(ip + "/24", strict=False)
        elif ipa.version == 6:
            isn = ipaddress.ip_network(ip + "/48", strict=False)
        else:
            isn = ipaddress.ip_network("::/64")
        return(str(isn))

    def add_to_dict(d, ip, n1, n2):
        sub = dgaBySubnet.subnet_from_ip(ip)
        if sub in d:
            d[sub].count1 += n1
            d[sub].count2 += n2
            d[sub].total += n1 + n2
        else:
            dbs = dgaBySubnet()
            dbs.sub = sub
            dbs.count1 = n1
            dbs.count2 = n2
            dbs.total = n1 + n2
            d[sub] = dbs

# main
l1 = dgaByIp.table_from_file(sys.argv[1])
l1.sort()
print("Found " + str(len(l1)) + " entries in " + sys.argv[1])
l2 = dgaByIp.table_from_file(sys.argv[2])
l2.sort()
print("Found " + str(len(l2)) + " entries in " + sys.argv[2])

i1 = 0
i2 = 0
nb_same = 0
nb_only1 = 0
nb_only2 = 0
subdict = dict()
while i1 < len(l1) and i2 < len(l2):
    if l1[i1].ip == l2[i2].ip:
        dgaBySubnet.add_to_dict(subdict, l1[i1].ip, l1[i1].count, l2[i2].count)
        i1 += 1
        i2 += 1
        nb_same += 1
    elif l1[i1].ip < l2[i2].ip:
        dgaBySubnet.add_to_dict(subdict, l1[i1].ip, l1[i1].count, 0)
        i1 += 1
        nb_only1 += 1
    else:
        dgaBySubnet.add_to_dict(subdict, l2[i2].ip, 0, l2[i2].count)
        i2 += 1
        nb_only2 += 1

while i1 < len(l1):
    dgaBySubnet.add_to_dict(subdict, l1[i1].ip, l1[i1].count, 0)
    i1 += 1
    nb_only1 += 1

while i2 < len(l2):
    dgaBySubnet.add_to_dict(subdict, l2[i2].ip, 0, l2[i2].count)
    i2 += 1
    nb_only2 += 1

print("Same: " + str(nb_same))
print("Only 1: " + str(nb_only1))
print("Only 2: " + str(nb_only2))
print("Nb subnets: " + str(len(subdict)))
print("subnet, total, verisign, imrs-par")
for sub in subdict:
    print(subdict[sub].sub + ", " + str(subdict[sub].total) + ", " + str(subdict[sub].count1) + ", " + str(subdict[sub].count2))

