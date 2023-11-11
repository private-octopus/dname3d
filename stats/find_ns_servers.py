#!/usr/bin/python
# coding=utf-8
#
# This script collects data on sites listed in a master file such as the "magnificent million".
# The strategy is to pick at random names of small and large files, get the data, and add it
# to the result file. Running the program in the background will eventually accumulate enough
# data to do meaning ful statistics.

import sys
import traceback
import pubsuffix
import ip2as
import dnslook
import ns_store
import dns_bucket
import zoneparser
import time


# Main
def main():
    start_time = time.time()
    if len(sys.argv) != 10 and len(sys.argv) != 11:
        print("Usage: " + sys.argv[0] + " nb_trials ip2as.csv ip2as6.csv publicsuffix.dat asn_file million_domain_list million_ns_list [tmp_prefix]")
        exit(1)
    try:
        nb_trials = int(sys.argv[1])
    except Exception as e:
        print("Incorect nb_trials value (" + sys.argv[1] + "), exception: " + str(e))
        exit(1)
    fixed_weight = True
    ip2as_file = sys.argv[2]
    ip2as6_file = sys.argv[3]
    public_suffix_file = sys.argv[4]
    dups_file = sys.argv[5]
    asn_file = sys.argv[6]
    million_file = sys.argv[7]
    ns_file = sys.argv[8]
    if len(sys.argv) == 10:
        temp_prefix = sys.argv[9]
    else:
        temp_prefix = ""

    ps = pubsuffix.public_suffix()
    if not ps.load_file(public_suffix_file):
        print("Could not load the suffixes")
        exit(1)
    
    # get the AS names
    asns = ip2as.asname()
    if not asns.load(asn_file):
        exit(-1)
    asn_ag = ip2as.aggregated_asn()
    i2a = ip2as.load_ip2as(ip2as_file)
    i2a6 = ip2as.load_ip2as(ip2as6_file)
    
    # load the current ns list
    nd = ns_store.ns_dict()
    try:
        nd.load_ns_file(ns_file, dot_after=10000)
    except Exception as e:
        print("Could not load " + ns_file + ", exception: " + str(e))

    print("NS list has " + str(len(nd.d)) + " entries, scanning millions.")

    # parse the million file for additional NS records, but do not
    # actually load it. Loading it would increase the memory footprint
    # of the process by maybe 1GB. Not a problem per se, but we will
    # then fork N processes for the "bucket" evaluation, for a combined
    # memory foot print of maybe 256GB, and that would not to work well.
    dnsl_loaded = 0
    dot_after = 10000
    for line in open(million_file, "rt"):
        dnsl = dnslook.dnslook()
        try:
            dnsl.from_json(line)
            for ns in dnsl.ns:
                nd.add_ns_name(ns)
            dnsl_loaded += 1
        except Exception as e:
            traceback.print_exc()
            print("Cannot parse <" + line  + ">\nException: " + str(e))
        if dot_after > 0 and dnsl_loaded%dot_after == 0:
            sys.stdout.write(".")
            sys.stdout.flush()
    print("\nParsed " + str(dnsl_loaded) + " domains from million file.")
    
    targets = nd.random_list(nb_trials, only_news=True)
    print("Selected " + str(len(targets)) + " targets out of " + str(len(nd.d)))
    file_time = time.time()
    print("Loaded the files in " + str(file_time - start_time) + " seconds.")
    
    # add the ns records from the million file to the ns list:
    stats = [ 0, 0, 0, 0, 0, 0, 0]
    dl = dns_bucket.bucket_list(nd.d, targets, ps, i2a, i2a6, temp_prefix, "_ns.csv", "_stats.csv")
    dl.run()
    nd.save_ns_file(ns_file)
    print("NS list has " + str(len(nd.d)) + " entries")

# actual main program, can be called by threads, etc.
if __name__ == '__main__':
    main()