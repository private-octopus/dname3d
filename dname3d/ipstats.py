#!/usr/bin/python
# coding=utf-8
#
# Parse the names file and extract statistics similar to sum3,
# but also including the dga13 category.

import traceback
import namestats
import sys
import ipaddress

class dga13net:
    def __init__(self, net, count):
        self.net = net
        self.count = count
        self.number = 1
        self.c_min = count
        self.c_max = count

    def add(self,count):
        self.count += count
        self.number += 1
        if self.c_min > count:
            self.c_min = count
        if self.c_max < count:
            self.c_max = count

    def to_str(self):
        s = self.net
        s += "," + str(self.count) 
        s += "," + str(self.number)
        s += "," + str(self.c_min)
        s += "," + str(self.c_max)
        return s

    def myComp(self, other):
        if (self.count < other.count):
            return -1
        elif (self.count > other.count):
            return 1
        elif (self.c_min < other.c_min):
            return -1
        elif (self.c_min > other.c_min):
            return 1
        elif (self.c_max < other.c_max):
            return -1
        elif (self.c_max > other.c_max):
            return 1
        elif (self.number < other.number):
            return -1
        elif (self.number > other.number):
            return 1
        elif (self.net < other.net):
            return -1
        elif (self.net > other.net):
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

# main

imported = sys.argv[1]

stats = namestats.namestats()
subnet0 = ipaddress.ip_network("::/64")
dga_nets = dict()

stats.import_result_file(imported)

print("file " + imported + " contains " + str(len(stats.dga_ip)) + " IP addresses")
total_count = 0

for ip in stats.dga_ip:
    try:
        count = stats.dga_ip[ip]
        total_count += count
        ipa = ipaddress.ip_address(ip)
        isn = subnet0
        if ipa.version == 4:
            isn = ipaddress.ip_network(ip + "/24", strict=False)
        elif ipa.version == 6:
            isn = ipaddress.ip_network(ip + "/56", strict=False)
        net = str(isn)
        if net in dga_nets:
            dga_nets[net].add(count)
        else:
            dga_nets[net] = dga13net(net, count)
    except Exception as exc:
        traceback.print_exc()
        print('\nIP %s generated an exception: %s' % (ip, exc))

print("Found " + str(len(dga_nets)) + " subnets")

net_list = list(dga_nets.values())

#for net in dga_nets:
#    net_list.append(dga_nets[net])
print("Listed " + str(len(net_list)) + " subnets")

net_list.sort(reverse=True)
print("Sorted " + str(len(net_list)) + " subnets")

if len(sys.argv) >= 3:
    nb = 0
    f = open(sys.argv[2] , "wt", encoding="utf-8")
    for net13 in net_list:
        f.write(net13.to_str()+"\n")
        nb+=1
    f.close()
    print("Wrote " + str(nb) + " lines in " + sys.argv[2])
else:
    verif_count = 0
    verif_target = int(95*total_count/100)
    for net13 in net_list:
        print(net13.to_str())
        verif_count += net13.count
        if verif_count >= verif_target:
            break
    print("Total " + str(verif_count) + ", expected " + str(total_count))