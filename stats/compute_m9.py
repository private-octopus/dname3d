#
# Produce the metric M9
#

import sys
import traceback
import pubsuffix
import ip2as
import dnslook
import ns_store
#import random
import zoneparser
#import time

class key_range_item:
    def __init__(self, key, rng, weight):
        self.key = key
        self.rng = rng
        self.weight = weight

def key_list_to_M9(key_list, m9date, metric, key_per_name, top_set, F):
    total_weight = 0
    # First compute total and average
    for kri in key_list:
        total_weight += kri.weight
    if total_weight > 0:
        # then compute top50, top 90
        cumul_weight = 0
        weight50 = 50*total_weight / 100
        weight90 = 90*total_weight / 100
        top50 = 1
        top90 = 1
        for kri in key_list:
            cumul_weight += kri.weight
            if cumul_weight < weight50:
                top50 += 1
            if cumul_weight < weight90:
                top90 += 1
            else:
                break
        # write the values
        F.write(metric + ".1," + m9date +",v2.0, ," + str(key_per_name) + "\n")
        F.write(metric + ".2," + m9date + ",v2.0, ," + str(top50) + "\n")
        F.write(metric + ".3," + m9date + ",v2.0, ," + str(top90) + "\n")
        nb_written = 0
        for kri in key_list:
            fraction =  kri.weight / total_weight
            nb_written += 1
            if nb_written <= 10 or fraction >= 0.005 or kri.key in top_set :
                F.write(metric + ".4," + m9date + ",v2.0, " + str(kri.key) + "," + str(fraction) + "\n")
            if nb_written > 10 and fraction < 0.0005:
                break

class key_weights:
    def __init__(self):
        self.weight = dict()
        self.total = [0, 0, 0, 0, 0, 0, 0]
        self.nb_names = [0, 0, 0, 0, 0, 0, 0]

    # add a vector of weights for a set of keys
    def add_key_weight(self, key_set, key_weight,fixed_weight=False):
        w = 1.0
        if len(key_set) > 1 and not fixed_weight:
            w /= len(key_set)
        for key in key_set:
            if not key in self.weight:
                self.weight[key] = [ 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0 ]
            for i in range(0,7):
                if len(key_weight) > i:
                    self.weight[key][i] += key_weight[i]*w

    # add one domain name in million range rng
    def add_names(self, key_set, rng, fixed_weight=False):
        w = [ 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0 ]
        if rng >= 0 and rng < 7:
            w[rng] = 1.0
            self.add_key_weight(key_set, w, fixed_weight=fixed_weight)
            if len(key_set) > 0:
                self.nb_names[rng] += 1
                self.total[rng] += len(key_set)

    # we have weights per key. we define top keys as keys that
    # have either a max value in a specific column, or a max value
    # for the sum of columns 0 to 4 (100, 1000, 10000, 100000, 1M)
    def get_sorted_list(self, rng):
        key_list = []
        for key in self.weight:
            w = 0
            weights = self.weight[key]
            if rng >= 0 and rng < len(weights):
                w = weights[rng]
            elif rng < 0:
                for i in range(0,6):
                    if len(weights) > i:
                        w += weights[i]
            if w > 0:
                key_list.append(key_range_item(key, rng, w))

        sorted_list = sorted(key_list, key=lambda item: item.weight, reverse=True)
        return sorted_list

    # writing the metrics
    def weights_to_m9(self, m9date, metric, m_first, F, init_set=[], write_metric=True, fixed_weight=False):
        key_lists=[]
        top_set = set()
        for key in init_set:
            top_set.add(key)
        for i in range(0,6):
            key_lists.append(self.get_sorted_list(i))
        for i in range(0,6):
            kl = key_lists[i]
            nb = len(kl)
            if nb > 5:
                nb = 5
            for j in range(0,nb):              
                top_set.add(kl[j].key)
        if write_metric:
            for i in range(0,6):
                if self.nb_names[i] > 0:
                    key_per_name = self.total[i]/self.nb_names[i]
                    print(str(metric) + "." + str(m_first + i) + ": " +  str( self.total[i]) + ", " + str(self.nb_names[i]))
                    key_list_to_M9(key_lists[i], m9date, metric + "." + str(m_first + i), key_per_name, top_set, F)
        return top_set


def write_key_range_items(key_list, file_name, key_name, weight_name):
    with open(file_name, "w") as F:
        F.write(key_name + ", " + weight_name + "\n")
        for kri in key_list:
            F.write(str(kri.key) + "," + str(kri.weight) + "\n")
    print("Saved " + str(len(key_list)) + " " + key_name + " in " + file_name)

def print_top_key_range_items(key_list, list_name):
    print(list_name + " has : " + str(len(key_list)))
    nb_top = 0
    for kri in key_list:
        nb_top += 1
        print(str(nb_top) + ": " + str(kri.key) + "," + str(kri.weight))
        if nb_top >= 10:
            break;

def print_top_key_range_asn(key_list, list_name, asns):
    print(list_name + " has : " + str(len(key_list)))
    nb_top = 0
    for kri in key_list:
        nb_top += 1
        print(str(nb_top) + ": "  + str(kri.key) + ", " + asns.name(kri.key) + ", " + str(kri.weight))
        if nb_top >= 10:
            break;

def millions_to_suffix_weights(millions, ps, dups, fixed_weight):
    suffix_weights = key_weights()
    for dns_item in millions:
        ns_suffixes = set()
        for ns in dns_item.ns:
            ns_suffix = zoneparser.extract_server_suffix(ns, ps, dups)
            ns_suffixes.add(ns_suffix)
        suffix_weights.add_names(ns_suffixes, dns_item.million_range, fixed_weight=fixed_weight)
    return suffix_weights

def millions_to_ns_as_weights(millions, nd, asn_ag, fixed_weight):
    ns_as_weights = key_weights()
    for dns_item in millions:
        ns_as_numbers = set()
        for ns in dns_item.ns:
            if ns in nd.d:
                for asn in nd.d[ns].ases:
                    ns_as_numbers.add(asn_ag.get_asn(asn))
        ns_as_weights.add_names(ns_as_numbers, dns_item.million_range, fixed_weight=fixed_weight)
    return ns_as_weights

def millions_to_as_weights(millions, asn_ag, fixed_weight):
    as_weights = key_weights()
    for dns_item in millions:
        as_numbers = set()
        for asn in dns_item.ases:
            as_numbers.add(asn_ag.get_asn(asn))
        as_weights.add_names(as_numbers, dns_item.million_range, fixed_weight=fixed_weight)
    return as_weights

def compute_m9(millions, ps, dups, nd, asn_ag, asns, m9date, fixed_weight, F):
    suffix_weights = millions_to_suffix_weights(millions, ps, dups, fixed_weight)
    suffix_weights.weights_to_m9(m9date, "M9", 1, F)
    as_weights = millions_to_as_weights(millions, asn_ag, fixed_weight)
    top_as = as_weights.weights_to_m9(m9date, "M9", 13, F, write_metric=False)

    ns_as_weights = millions_to_ns_as_weights(millions, nd, asn_ag, fixed_weight)
    top_as = ns_as_weights.weights_to_m9(m9date, "M9", 7, F, top_as)
    as_weights.weights_to_m9(m9date, "M9", 13, F, top_as)
    for asn in top_as:
        F.write("M9.19.1," + m9date + ",v2.0, " + ip2as.asname.clean(asns.name(asn)) + "," + str(asn) + "\n")

def save_m9(millions, ps, dups, nd, asn_ag, asns, m9date, fixed_weight, file_name):
    with open(file_name, "w") as F:
        compute_m9(millions, ps, dups, nd, asn_ag, asns, m9date, fixed_weight, F)
    print("Save M9 metric for " + m9date + " in " + file_name)

# Main
def main():
    if len(sys.argv) != 9:
        print("Usage: " + sys.argv[0] + " publicsuffix.dat dups asn_file million_domain_list million_ns_list m9_metric_file m9_metric_file m9_day ip_file_prefix")
        exit(-1)
    fixed_weight = True
    public_suffix_file = sys.argv[1]
    dups_file = sys.argv[2]
    asn_file = sys.argv[3]
    million_file = sys.argv[4]
    ns_file = sys.argv[5]
    m9_metric_file = sys.argv[6]
    m9_metric_day = sys.argv[7]
    ip_file_prefix = sys.argv[8]

    # load public suffixes
    ps = pubsuffix.public_suffix()
    if not ps.load_file(public_suffix_file):
        print("Could not load the public  suffixes")
        exit(1)
    # load duplicate service names 
    zp = zoneparser.zone_parser2(ps)
    zp.load_dups(dups_file)
    # get the AS names
    asns = ip2as.asname()
    if not asns.load(asn_file):
        exit(-1)
    asn_ag = ip2as.aggregated_asn()
    # load the current ns list
    nd = ns_store.ns_dict()
    try:
        nd.load_ns_file(ns_file, dot_after=10000)
    except Exception as e:
        print("Could not load " + ns_file + ", exception: " + str(e))
        exit(-1)
    print("NS list has " + str(len(nd.d)) + " entries, loading dns millions.")

    # Load the million file now.
    millions = dnslook.load_dns_file(million_file, dot_after=10000)
    print("\nLoaded " + str(len(millions)) + " domains from million file.")
    
    # Prepare the files of IP addresses
    ns_ip4_weight = key_weights()
    ns_ip6_weight = key_weights()
    for dns_item in millions:
        for ns in dns_item.ns:
            if ns in nd.d:
                ns_ip4_weight.add_names(nd.d[ns].ip, dns_item.million_range, fixed_weight=fixed_weight)
                ns_ip6_weight.add_names(nd.d[ns].ipv6, dns_item.million_range, fixed_weight=fixed_weight)

    print("After loading from NS, NS_IP4 weights has " + str(len(ns_ip4_weight.weight)) + " entries")
    print("After loading from NS, NS_IP6 weights has " + str(len(ns_ip4_weight.weight)) + " entries")

    top_ns_ip4 = ns_ip4_weight.get_sorted_list(-1)
    print_top_key_range_items(top_ns_ip4, "top_ns_ip4")
    write_key_range_items(top_ns_ip4, ip_file_prefix + "_ip4.csv", "ipv4", "weight")
    top_ns_ip6 = ns_ip6_weight.get_sorted_list(-1)
    print_top_key_range_items(top_ns_ip6, "top_ns_ip6")
    write_key_range_items(top_ns_ip6, ip_file_prefix + "_ip6.csv", "ipv6", "weight")

    # produce M9
    save_m9(millions, ps, zp.dups, nd, asn_ag, asns, m9_metric_day, fixed_weight, m9_metric_file)

# actual main program, can be called by threads, etc.
if __name__ == '__main__':
    main()