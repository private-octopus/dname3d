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
import ip2as

class asn_or_net:
    def __init__(self, name):
        self.name = name
        self.w = [ 0.0, 0.0, 0.0, 0.0, 0.0, 0.0 ]


def add_to_asn_or_net(n_dict, name, weight, million_range):
    try:
        if million_range >= 0 and million_range < 6:
            if not name in n_dict:
                n_dict[name] = asn_or_net(name)
            n_dict[name].w[million_range] += weight
    except Exception as e:
        traceback.print_exc()
        print("Exception: " + str(e))
        print("name: " + str(name))
        print("Entries in dict: " + str(len(n_dict)))
        exit(-1)

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

def add_aggregated_asn(asn_dict, aggregator, raw_asn_list, million_range):
    a_list = set()
    for asn in raw_asn_list:
        new_asn = aggregator.get_asn(asn)
        a_list.add(new_asn)
    add_list_of_asn_or_net(asn_dict, a_list, million_range)

# recompute the set of ASN to make sure bith IPv4 and IPv6 addresses
def recompute_asn(dnslook_entry, i2a, i2a6):
    if len(i2a.table) > 0 and len(i2a6.table) > 0:
        dnslook_entry.get_asn(i2a, i2a6)

def add_dnslook_entry(net_dict, asn_dict, aggregator, dnslook_entry):
    add_list_of_nets(net_dict, dnslook_entry.ip, dnslook_entry.ipv6, dnslook_entry.million_range)
    add_aggregated_asn(asn_dict, aggregator, dnslook_entry.ases, dnslook_entry.million_range)

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

def write_asn_list(asn_dict, asns, threshold, file_name):
    w_cat = [ 0, 0, 0, 0, 0, 0 ]
    for asn in asn_dict:
        for r in range(0,6):
            w_cat[r] += asn_dict[asn].w[r]
    with open(file_name, "wt") as F:
        F.write("asn, name")
        for r in range(0,6):
            F.write(", share[" + str(r) + "], weight[" + str(r) + "]")
        F.write("\n")
        for asn in asn_dict:
            selected = False
            for r in range(1,6):
                if asn_dict[asn].w[r] > threshold:
                    selected = True
                    break
            if selected:
                F.write(str(asn) + ", \"" + asns.name(asn) +"\"")
                for r in range(0,6):
                    F.write(", " + str(asn_dict[asn].w[r]/w_cat[r]) + "," + str(asn_dict[asn].w[r]))
                F.write("\n")

# Main entry point
if len(sys.argv) != 8 :
    print("Usage: " + sys.argv[0] + " million_file asn_names net_file asn_file big_asn_file i2as_file i2as6_file")
    exit(-1)

million_file = sys.argv[1]
asn_names_file = sys.argv[2]
net_file = sys.argv[3]
asn_file = sys.argv[4]
big_asn_file = sys.argv[5]
ip2as_file = sys.argv[6]
ip2as6_file = sys.argv[7]
aggregator = ip2as.aggregated_asn()
# get the AS names
asns = ip2as.asname()
if not asns.load(asn_names_file):
    exit(-1)
    
i2a = ip2as.load_ip2as(ip2as_file)
i2a6 = ip2as.load_ip2as(ip2as6_file)

print("Aggregator.get_asn(13335) = " + str(aggregator.get_asn(13335)))

# save the values
mf = dnslook.load_dns_file(million_file)
print("!")
net_list = dict()
asn_list = dict()
for dnslook_entry in mf:
    recompute_asn(dnslook_entry, i2a, i2a6)
    add_dnslook_entry(net_list,asn_list,  aggregator, dnslook_entry)
print("!")
write_list(net_list, 0.001, "network", net_file)
write_asn_list(asn_list, asns, 0.001, asn_file)

tracked_asn = [ "GOOGLE", "AKAMAI", "AMAZON", "CLOUDFLARE", "FASTLY", "MICROSOFT",
                "AWS", "AZURE", "AUTOMATTIC", "NAMECHEAP", "IPINFO", "OVH", "UNIFIED", "IONOS",
                "SQUARESPACE", "ALIBABA", "HETZNER", "DIGITALOCEAN", "CONFLUENCE",
                "INCAPSULA", "NEWFOLD", "HOSTINGER", "NETWORKSOLUTIONS",
                "FACEBOOK" ]
n_asn = 0
n_big = 0
with open(big_asn_file, "wt") as F:
    for asn in asn_list:
        asn_nb = int(asn)
        name = asns.name(asn_nb)
        n_asn += 1
        for prefix in tracked_asn:
            if name.startswith(prefix):
                F.write(str(asn_nb) + ", " + name + "\n")
                n_big += 1
                break
    print("Found " + str(n_asn) + " ASes, " + str(n_big) + " bigs.")



