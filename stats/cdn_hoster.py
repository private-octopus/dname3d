#!/usr/bin/python
# coding=utf-8
#
# The looks at the list of domain names in the dns million
# file of the month, and retain the names for which at least one
# IP address is documented. For each entry, it builds a list
# of network prefixes and a list of ASN. Then, it adds an entry
# to the network or ASN dictionary, and adds weight 1/number of
# network or ASN to that entry.

import sys
import dnslook
import traceback
import ipaddress

class asn_or_net:
    def __init__(self, name):
        self.name = name
        self.w = [ 0.0, 0.0, 0.0, 0.0, 0.0, 0.0 ]


def add_to_asn_or_net(n_dict, name, weight, million_range):
    if million_range > 0 and million_range < 6:
        if not name in n_dict:
            n_dict[name] = asn_or_net(name)
        n_dict[name].w[million_range] += weight

def add_list_of_asn_or_net(n_dict, n_list, million_range):
    if len(n_list) > 0:
        weight = 1.0/len(n_list)
        for name in n_list:
            add_to_asn_or_net(n_dict, name, weight, million_range)

def add_list_of_nets(net_dict, ip_list, ip_v6_list, million_range):
    n_list = set()
    for ip in ip_list:
        try:
            prf4 = ipaddress.IPv4Network(ip + "/24", strict=False)
            net24 = str(prf4)
            n_list.add(net24)
        except:
            print("Cannot parse IPv4 = " + ip)
    for ip6 in ip_v6_list:
        try:
            prf6 = ipaddress.IPv6Network(ip6 + "/48", strict=False)
            net48 = str(prf6)
            n_list.add(net48)
        except:
            print("Cannot parse IPv6 = " + ip6)
    add_list_of_asn_or_net(net_dict, n_list, million_range)

def add_dnslook_entry(net_dict, asn_dict, dnslook_entry):
    add_list_of_nets(net_dict, dnslook_entry.ip, dnslook_entry.ipv6, dnslook_entry.million_range)
    add_list_of_asn_or_net(asn_dict, dnslook_entry.ases, dnslook_entry.million_range)

def write_list(n_dict, threshold, head_name, file_name):
    with open(file_name, "wt") as F:
        F.write(head_name + ", range, weight, share,\n")
        for million_range in range(0,6):
            w_cat = 0
            for name in n_dict:
                w_cat += n_dict[name].w[million_range]
            if w_cat > 0:
                high_threshold = threshold*w_cat
                other_weight = 0.0
                for name in n_dict:
                    if n_dict[name].w[million_range] > high_threshold:
                        F.write(name + "," + str(million_range) + "," + str(n_dict[name].w[million_range]) + "," + str(n_dict[name].w[million_range]/w_cat) + ",\n")

# Main entry point
if len(sys.argv) != 4 :
    print("Usage: " + sys.argv[0] + " million_file net_file asn_file")
    exit(-1)

million_file = sys.argv[1]
net_file = sys.argv[2]
asn_file = sys.argv[3]

mf = dnslook.load_dns_file(million_file)
net_list = dict()
asn_list = dict()
for dnslook_entry in mf:
    add_dnslook_entry(net_list, asn_list, dnslook_entry)
write_list(net_list, 0.001, "network", net_file)
write_list(net_list, 0.001, "asn", net_file)


