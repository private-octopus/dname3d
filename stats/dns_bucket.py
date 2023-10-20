# 
# DNS bucket -- organize parallel lookup of DNSlook records.
#

import dnslook
import concurrent.futures
import time
import os
import sys
import traceback

def load_name(item_dict, target, ps, i2a, i2a6, stats):
    success = False
    if target.domain in item_dict:
        d = item_dict[target.domain]
    else:
        d = dnslook.dnslook()
        d.domain = target.domain
        d.million_rank = target.million_rank
        d.million_range = target.million_range
    try:
        if d.nb_queries == 0:
            # Get the name servers, etc. 
            d.get_domain_data(target.domain, ps, i2a, i2a6, stats, rank=target.million_rank, rng=target.million_range)
        else:
            d.retry_domain_data(ps, i2a, i2a6, stats)
        success = True
    except Exception as e:
        traceback.print_exc()
        print("Cannot assess domain <" + target.domain  + ">\nException: " + str(e))
    return success, d

class dns_lookup_bucket:
    def __init__(self, bucket_id, item_dict, bucket_file_name, stats_file_name, targets, ps, i2a, i2a6):
        self.bucket_id = bucket_id
        self.item_dict = item_dict
        self.targets = targets
        self.bucket_file_name = bucket_file_name
        self.ps = ps
        self.i2a = i2a
        self.i2a6 = i2a6
        self.stats = [ 0, 0, 0, 0, 0, 0, 0 ]
        self.stats_file_name = stats_file_name
        self.is_complete = False

    def load(self):     
        with open(self.bucket_file_name, "wt") as f_out:
            for target in self.targets:
                success, d = load_name(self.item_dict, target, self.ps, self.i2a, self.i2a6, self.stats)
                if success:
                    # Write the json line in the result file.
                    f_out.write(d.to_json() + "\n")
        sys.stdout.flush()
        with open(self.stats_file_name,"wt") as f_stats:
            for stat in self.stats:
                f_stats.write(str(stat) + "\n")

    def aggregate(self,stats):
        for line in open(self.bucket_file_name, "rt"):
            js_line = line.strip()
            if len(js_line) > 0:
                w = dnslook.dnslook()
                if w.from_json(js_line):
                    self.item_dict[w.domain] = w
                else:
                    print("Cannot parse result line " + line.strip())
                    continue
        stats_index = 0
        for line in open(self.stats_file_name, "rt"):
            st = float(line.strip())
            stats[stats_index] += st
            stats_index += 1
            if stats_index >= len(stats):
                break

def load_dns_look_up_bucket(bucket):
    bucket.load()

class bucket_list:
    def __init__(self, item_dict, targets, ps, i2a, i2a6, temp_prefix, temp_suffix, stats_suffix):
        self.item_dict = item_dict
        self.target_count_per_bucket = []
        self.targets = targets
        self.temp_prefix = temp_prefix
        self.temp_suffix = temp_suffix
        self.stats_suffix = stats_suffix
        self.bucket_list = []
        self.stats = [ 0, 0, 0, 0, 0, 0, 0 ]
        self.ps = ps
        self.i2a = i2a
        self.i2a6 = i2a6
        self.nb_process = 1

    def prepare_target_list(self):
        self.nb_process = os.cpu_count()
        targets_left = len(self.targets)
        # split the target with units per processor
        for bucket_id in range(0,self.nb_process):
            buckets_left = self.nb_process - bucket_id
            if buckets_left == 1:
                nb_this_bucket = targets_left
            else:
                nb_this_bucket = int(targets_left/buckets_left)
                if nb_this_bucket == 0:
                    nb_this_bucket = 1
            self.target_count_per_bucket.append(nb_this_bucket)
            targets_left -= nb_this_bucket
            if targets_left <= 0:
                break
        print("Prepared: " + str(len(self.target_count_per_bucket)) + " target lists.")

    def prepare_buckets(self):
        bucket_list = []
        last_target = 0
        old_target = 0;
        for bucket_id in range(0,len(self.target_count_per_bucket)):
            temp_name = self.temp_prefix + str(bucket_id) + self.temp_suffix
            temp_stats =  self.temp_prefix + str(bucket_id) + self.stats_suffix
            old_target = last_target
            last_target += self.target_count_per_bucket[bucket_id]
            bucket_target = self.targets[old_target:last_target]
            this_bucket = dns_lookup_bucket(bucket_id, self.item_dict, temp_name, temp_stats, bucket_target, self.ps, self.i2a, self.i2a6)
            self.bucket_list.append(this_bucket)
        print("Prepared: " + str(len(self.bucket_list)) + " buckets.")

    def run_buckets(self):
        with concurrent.futures.ProcessPoolExecutor(max_workers = self.nb_process) as executor:
            future_to_bucket = { executor.submit(load_dns_look_up_bucket, bucket):bucket for bucket in self.bucket_list }
        print("Started: " + str(len(future_to_bucket)) + " buckets.")
        for future in concurrent.futures.as_completed(future_to_bucket):
            bucket = future_to_bucket[future]
            try:
                data = future.result()
                bucket.is_complete = True
                sys.stdout.write(".")
                sys.stdout.flush()
            except Exception as exc:
                traceback.print_exc()
                print("\nBucket " + str(bucket.bucket_id) + " generated an exception: " + str(exc))

    def aggregate(self):
        for bucket in self.bucket_list:
            if not bucket.is_complete:
                continue
            bucket.aggregate(self.stats)

    def run(self):
        start_time = time.time()
        ready_time = start_time
        bucket_time = start_time
        done_time = start_time
        if self.temp_prefix == "":
            for target in self.targets:
                success, d = load_name(self.item_dict, target, self.ps, self.i2a, self.i2a6, self.stats)
                if success:
                    self.item_dict[d.domain] = d
            done_time = time.time()
        else:
            self.prepare_target_list()
            print("Targets: " + str(len(self.targets)) + ", buckets: " + str(len(self.target_count_per_bucket)) + " (" + str(self.target_count_per_bucket[0]) + "..." + str(self.target_count_per_bucket[-1]) + ")")  
            ready_time = time.time()
            self.prepare_buckets()
            self.run_buckets()
            bucket_time = time.time()
            self.aggregate()
            done_time = time.time()
            print("\nSummary took " + str(done_time - bucket_time))
        nb_assessed = len(self.targets)
        print("Assessed " + str(len(self.targets)) + " targets in " + str(done_time - start_time))  
        stat_name = ["a", "aaaa", "ns", "algo", "cname", "server", "asn"]
        if nb_assessed < 1:
            nb_assessed = 1
        for x in range(0,7):
            if len(self.stats) > x:
                print("Time " + stat_name[x] + ": " + str(self.stats[x]/nb_assessed))

        
 
