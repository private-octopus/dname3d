#!/usr/bin/python
# coding=utf-8
#
# This script collects data on sites listed in a master file such as the "magnificent million".
# The strategy is to pick at random names of small and large files, get the data, and add it
# to the result file. Running the program in the background will eventually accumulate enough
# data to do meaning ful statistics.

import sys
import dns.resolver
import json
import traceback
import pubsuffix
import ip2as
import dnslook
import random
import million_random
import time
import concurrent.futures
import os

def load_names(result_file, targets, ps, i2a, i2a6, stats): 
    with open(result_file, "wt") as f_out:
        for target in targets:
            try:
                d = dnslook.dnslook()
                # Get the name servers, etc. 
                d.get_domain_data(target.domain, ps, i2a, i2a6, stats, rank=target.million_rank, rng=target.million_range)
                # Write the json line in the result file.
                f_out.write(d.to_json() + "\n")
            except Exception as e:
                traceback.print_exc()
                print("Cannot assess domain <" + target.domain  + ">\nException: " + str(e))
                break


class dns_lookup_bucket:
    def __init__(self, bucket_id, bucket_file_name, stats_file_name, targets, ps, i2a, i2a6, stats):
        self.bucket_id = bucket_id
        self.targets = targets
        self.bucket_file_name = bucket_file_name
        self.ps = ps
        self.i2a = i2a
        self.i2a6 = i2a6
        self.stats = stats
        self.stats_file_name = stats_file_name
        self.is_complete = False

    def load(self):
        load_names(self.bucket_file_name, self.targets, self.ps, self.i2a, self.i2a6, self.stats)
        sys.stdout.flush()
        with open(self.stats_file_name,"wt") as f_stats:
            for stat in self.stats:
                f_stats.write(str(stat) + "\n")

def load_dns_look_up_bucket(bucket):
    bucket.load()


# Main
def main():
    start_time = time.time()
    if len(sys.argv) != 8 and len(sys.argv) != 9:
        print("Usage: " + sys.argv[0] + " nb_trials ip2as.csv ip2as6.csv publicsuffix.dat million_domain_list com_sample result_file [tmp_prefix]")
        exit(1)
    nb_trials = int(sys.argv[1])
    ip2as_file = sys.argv[2]
    ip2as6_file = sys.argv[3]
    public_suffix_file = sys.argv[4]
    million_file = sys.argv[5]
    com_sample = sys.argv[6]
    result_file = sys.argv[7]
    if len(sys.argv) == 9:
        temp_prefix = sys.argv[8]
    else:
        temp_prefix = ""

    ps = pubsuffix.public_suffix()
    if not ps.load_file(public_suffix_file):
        print("Could not load the suffixes")
        exit(1)

    i2a = ip2as.load_ip2as(ip2as_file)
    i2a6 = ip2as.load_ip2as(ip2as6_file)

    mr = million_random.million_random(100, 10)
    already_found = set()
    duplicates_found = 0
    # Read the names already in the result file, remove them from consideration.
    try:
        for line in open(result_file, "rt", encoding="utf-8"):
            js_line = line.strip()
            if len(js_line) > 0:
                w = dnslook.dnslook()
                if w.from_json(js_line):
                    if not w.domain in already_found:
                        already_found.add(w.domain)
                        mr.set_already_processed(w.domain)
                    else:
                        duplicates_found += 1
                else:
                    print("Cannot parse result line " + line.strip())
                    print("Closing " + result_file + " and exiting.")
                    exit(1)
    except FileNotFoundError:
        # doesn't exist
        print("File " + result_file + " will be created.")
    except Exception as e:
        traceback.print_exc()
        print("Cannot load file <" + result_file  + ">\nException: " + str(e))
        print("Giving up");
        exit(1)
    print("Loaded " + str(len(mr.already_processed)) + " names, " + str(duplicates_found) + " duplicates.")


    # Load the million file. The loading process will not load the names 
    # marked as already processed in the previous loop.
    mr.load(million_file)
    # Add the names from the zone sample file
    mr.load_zone_sample(com_sample)
    # Show the state of the random loader
    for a in range(0, len(mr.range_names)):
        print("Range " + str(a) + ", " + str(len(mr.range_names[a])) + " names")
    # Once everything is ready, start getting the requested number of new names
    # The names are picked at random from five zones in the million names list
    # as encoded in the "million_random" class
    pick_start = time.time()
    print("Ready after " + str(pick_start - start_time))
    stat_name = ["a", "aaaa", "ns", "algo", "cname", "server", "asn"]
    stats = []
    for x in range(0,7):
        stats.append(0)
    targets = []
    while len(targets) < nb_trials:
        target = mr.random_pick()
        if target.domain == "":
            if mr.nb_names() == 0:
                # no other empty range
                print("Error. All ranges empty after " + str(len(targets) + 1) + " trials, but loop did not stop.")
                break
            else:
                target =  mr.random_pick()
                if x.domain == "":
                    print("Error. Range " + str(mr.random_range) + " is empty, yet was picked.")
                    break
        else:
            targets.append(target)
            mr.mark_read(target)
        if mr.nb_names() == 0:
            # no other empty range
            print("All ranges empty after " + str(len(targets) + 1) + " trials.")
            break
    nb_assessed = len(targets)

    if temp_prefix != "":
        target_file = temp_prefix + "_targets.txt"
        with open(target_file, "wt") as tf:
            for target in targets:
                tf.write(target.domain + "," + str(target.million_rank) + "," + str(target.million_range) + "\n");

    # Once the required number of targets has been selected, prepare parallel threads
    ready_time = time.time()
    if temp_prefix == "":
        load_names(result_file, targets, ps, i2a, i2a6, stats)
        done_time = time.time()
        print("\nQueries took " + str(done_time - ready_time))
    else:
        nb_process = os.cpu_count()
        target_count_per_bucket = []
        targets_left = len(targets)
        # split the target with units per processor
        for bucket_id in range(0,nb_process):
            buckets_left = nb_process - bucket_id
            if buckets_left == 1:
                nb_this_bucket = targets_left
            else:
                nb_this_bucket = int(targets_left/buckets_left)
                if nb_this_bucket == 0:
                    nb_this_bucket = 1
            target_count_per_bucket.append(nb_this_bucket)
            targets_left -= nb_this_bucket
            if targets_left <= 0:
                break
        print("Targets: " +str(nb_assessed) + ", buckets: " + str(len(target_count_per_bucket)) + " (" + str(target_count_per_bucket[0]) + "..." + str(target_count_per_bucket[-1]) + ")")
        bucket_list = []
        last_target = 0
        for bucket_id in range(0,len(target_count_per_bucket)):
            temp_name = temp_prefix + str(bucket_id) + "_dns_results.csv"
            temp_stats =  temp_prefix + str(bucket_id) + "_stats.csv"
            old_target = last_target
            last_target += target_count_per_bucket[bucket_id]
            bucket_target = targets[old_target:last_target]
            this_bucket = dns_lookup_bucket(bucket_id, temp_name, temp_stats, bucket_target, ps, i2a, i2a6, stats)
            bucket_list.append(this_bucket)
        # run multiple parsing in parallel
        with concurrent.futures.ProcessPoolExecutor(max_workers = nb_process) as executor:
            future_to_bucket = {executor.submit(load_dns_look_up_bucket, bucket):bucket for bucket in bucket_list }
            for future in concurrent.futures.as_completed(future_to_bucket):
                bucket = future_to_bucket[future]
                try:
                    data = future.result()
                    bucket.is_complete = True
                    sys.stdout.write(".")
                    sys.stdout.flush()
                except Exception as exc:
                    traceback.print_exc()
                    print('\nBucket %d generated an exception: %s' % (bucket.bucket_id, exc))

        bucket_time = time.time()
        print("\nThreads took " + str(bucket_time - ready_time))
        # aggregate the results
        with open(result_file, "at") as f_out:
            for bucket in bucket_list:
                if not bucket.is_complete:
                    continue
                b_duplicates_added = 0
                b_domains_added = 0
                # load the domain names from the partial result files
                # we take care to not add duplicates
                for line in open(bucket.bucket_file_name, "rt"):
                    js_line = line.strip()
                    if len(js_line) > 0:
                        w = dnslook.dnslook()
                        if w.from_json(js_line):
                            if not w.domain in already_found:
                                already_found.add(w.domain)
                                f_out.write(line);
                        else:
                            print("Cannot parse result line " + line.strip())
                            continue
                stats_index = 0
                for line in open(bucket.stats_file_name, "rt"):
                    st = float(line.strip())
                    stats[stats_index] += st
                    stats_index += 1
                    if stats_index >= len(stats):
                        break
        done_time = time.time()
        print("\nSummary took " + str(done_time - bucket_time))

    print("Assessed " + str(nb_assessed) + " domains in " + str(done_time - start_time))
    for x in range(0,7):
        print("Time " + stat_name[x] + ": " + str(stats[x]/nb_assessed))

# actual main program, can be called by threads, etc.
if __name__ == '__main__':
    main()