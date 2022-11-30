#!/usr/bin/python
# coding=utf-8
# 
# Manage a list of servers, and obtain their addresses.
# This is used for example to draw statistics on name
# servers. Each name server object includes:
# - Name
# - Date IPv4 queried
# - Date IPv6 queried
# - List of IPv4 addresses (text representation)
# - List of IPv6 addresses (text representation)
# - List of network prefixes found in the IP addresses
#
# The names are loaded either from a text file or through an API.
# The loading process ensures that a given name occurs only once.
#
# In production, a script runs every day, with a new file
# created each month. The script runs after `do_dnslookup.py`,
# which every day obtains data for a new set of samples from
# the million list. This sampling may find new server names,
# not presnet in the current file. The order of operation
# will be:
# 
# - if in a new month, create a new file for that month.
# - load the current server list, as computed yesterday,
# - load the last "dns million" sample list.
# - extract server names from the dns million list,
#   and add these names to the current server list
# - find the names for which the IPv4 or IPv6 addresses
#   are not known.
# - perform DNS queries and store results in the corresponding
#   server name object.
# - for all names in the sample list, create an new
#   name + prefix record, with name + set of prefixes found.
#
# On a multicore machine, we will have to option of using multiple
# processes to perform DNS queries in parallel. This require a
# parallel version of the steps listed above:
#
# - get the list of necessary queries
# - split that list into one sublist per process
# - run the queries in separate processes
# - save queries results at the end of each process
# - read these results in the main process and add them to
#   the object in the server list.


from distutils.command.build_clib import build_clib
import sys
import dns.resolver
import json
import traceback
import pubsuffix
import ip2as
import dnslook
import million_random
import server_name_list
import time
import concurrent.futures
import os

def solve_dns_ns_bucket(bucket):
    bucket.solve()
    bucket.save(bucket.target_file)
    bucket.is_complete = True
    return(True)

# Main
def main():
    start_time = time.time()
    if len(sys.argv) != 5 and len(sys.argv) != 6:
        print("Usage: " + sys.argv[0] + " million_sample_file ns_cache_file sample_ns_prefix_file million_domain_list [ns_cache_bucket_prefix]")
        exit(1)
    million_sample_file = sys.argv[1]
    ns_cache_file = sys.argv[2]
    sample_ns_prefix_file = sys.argv[3]
    million_domain_list = sys.argv[4]
    ns_cache_bucket_prefix = ""
    if len(sys.argv) == 6:
        ns_cache_bucket_prefix = sys.argv[5]

    # load the current server list, as computed yesterday,
    # if in a new month, create a new ns cache file for that month.
    nl = server_name_list.name_entry_list()
    if os.path.exists(ns_cache_file):
        if not nl.load(ns_cache_file):
            print("File <" + ns_cache_file + "> could not be loaded")
            exit(1)
    else:
        print("File <" + ns_cache_file + "> not created yet")

    # load the million sample file, which contains the names of the
    # top servers included by the monthly M9 sampling process
    sample_names = []
    try:
        for line in open(million_sample_file, "rt", encoding="utf-8"):
            js_line = line.strip()
            if len(js_line) > 0:
                w = dnslook.dnslook()
                if w.from_json(js_line):
                    sample_names.append(w)
                else:
                    print("Cannot parse million sample line " + line.strip())
                    print("Closing " + million_sample_file + " and exiting.")
                    exit(1)
    except Exception as e:
        traceback.print_exc()
        print("Cannot load file <" + million_sample_file  + ">\nException: " + str(e))
        print("Giving up");
        exit(1)
    print("Found " + str(len(sample_names)) + " samples.")

    # extract server names from the dns million list,
    # and add these names to the current server list
    for w in sample_names:
        for ns in w.ns:
            nl.add_name(ns)
    print("Found " + str(len(nl.names)) + " separate NS in samples.")

    # - find the names for which the IPv4 or IPv6 addresses
    #   are not known.
    # - perform DNS queries and store results in the corresponding
    #   server name object.
    # TODO: if prefix defined, start parallel execution
    ready_time = time.time()
    if ns_cache_bucket_prefix == "":
        print ("Resolving names as single process.")
        nl.solve()
    else:
        # prepare parallel threads
        nb_process = os.cpu_count()
        bucket_list = nl.prepare_bucket(nb_process, ns_cache_bucket_prefix)
        print("Split names in " + str(len(bucket_list)) + " buckets")
        if len(bucket_list) > 0:
            # run multiple name solvers in parallel
            with concurrent.futures.ProcessPoolExecutor(max_workers = nb_process) as executor:
                future_to_bucket = {executor.submit(solve_dns_ns_bucket, bucket):bucket for bucket in bucket_list }
                for future in concurrent.futures.as_completed(future_to_bucket):
                    bucket = future_to_bucket[future]
                    try:
                        data = future.result()
                        sys.stdout.write(".")
                        sys.stdout.flush()
                    except Exception as exc:
                        traceback.print_exc()
                        print('\nBucket %d generated an exception: %s' % (bucket.bucket_id, exc))
            for bucket in bucket_list:
                nl.load(bucket.target_file)
            print("\nProcessed " + str(len(bucket_list)) + " buckets.")
    done_time = time.time()
    print("\nQueries took " + str(done_time - ready_time))
    nl.save(ns_cache_file)
    print("Saved " + ns_cache_file)

    # for all names in the sample list, create an new
    #   name + prefix record, with name + set of prefixes found.
    # Add rank from position in the million file

    mr = dict()
    million_rank = 0
    current_range = 0
    range_end = 100
    for line in open(million_domain_list, "rt"):
        domain = line.strip()
        million_rank += 1
        if million_rank >= range_end:
            current_range += 1
            range_end *= 10
        mr[domain] = current_range

    with open(sample_ns_prefix_file, "wt") as F:
        title = "rank,domain"
        for i in range(1,17):
            title += ",prefix" + str(i)
        title += ",\n"
        F.write(title)
        nb_saved = 0
        for w in sample_names:
            # Mark domain as incomplete if there are no name servers listed,
            # or if the IP address of the name servers is not available
            prefixes = set()
            incomplete = True
            for ns in w.ns:
                incomplete = False
                if ns in nl.names:
                    for prefix in nl.names[ns].prefixes:
                        if not prefix in prefixes:
                            prefixes.add(prefix)
                else:
                    incomplete = True
            if not incomplete:
                rank = -1
                if w.domain in mr:
                    rank = mr[w.domain]
                line = str(rank) + "," + w.domain
                for prefix in prefixes:
                    line += "," + prefix
                line += ",\n"
                F.write(line)
                nb_saved += 1
        print("Saved: "  + str(nb_saved) + " in " + sample_ns_prefix_file)

# actual main program, can be called by threads, etc.
if __name__ == '__main__':
    main()

            

