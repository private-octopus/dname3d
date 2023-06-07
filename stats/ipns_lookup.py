#!/usr/bin/python
# coding=utf-8
#
# The module nslookup builds a table of name servers for which
# there are references in the million file names or in other
# samples. For each reference name, it builds:
#
# - a list of IP and IPv6 addresses for that name
# - a list of ASes serving these IP or IPv6 addresses
#
import ipaddress
from netrc import netrc
import dnslook
import ip2as
import traceback
import sys
import concurrent.futures
import os

class ns_server_lookup_bucket:
    def __init__(self, bucket_id, bucket_file_name, targets, i2a):
        self.bucket_id = bucket_id
        self.targets = targets
        self.bucket_file_name = bucket_file_name
        self.i2a = i2a
        self.is_complete = False

    def load(self):      
        nt = dnslook.name_table()
        for name in self.targets:
            nt.add_name(name, self.i2a)
        nt.save(self.bucket_file_name)
        self.is_complete = True

def load_dns_look_up_bucket(bucket):
    bucket.load()

# Main entry point
def main():
    if len(sys.argv) < 5 or len(sys.argv) > 6:
        print("Usage: " + sys.argv[0] + " nb_lookup million_file ip2as_file result_file [temp_file_header]")
        exit(-1)
    try:
        nb_lookup = int(sys.argv[1])
    except:
        print("Incorrect number of max lookups: " + sys.argv[1])
        exit(1)

    million_file = sys.argv[2]
    ip2as_file = sys.argv[3]
    result_file = sys.argv[4]
    temp_prefix = ""
    if len(sys.argv) > 5:
        temp_prefix = sys.argv[5]

    mf = dnslook.load_dns_file(million_file)
    i2a = ip2as.ip2as_table()
    if i2a.load(ip2as_file):
        print("Loaded i2a table of length: " + str(len(i2a.table)))
    else:
        print("Could not load <" + ip2as_file + ">")
    nt = dnslook.name_table()
    if os.path.exists(result_file):
        nt.load(result_file)
        print("Loaded data for " + str(len(nt.table)) + " NS.")
    else:
        print("Will create: " + result_file)

    out_list = nt.schedule_ns(mf)
    print("Found " + str(len(out_list)) + " names")
    if len(out_list) > nb_lookup and nb_lookup > 0:
        print("Truncating to " + str(nb_lookup) + " names")
        out_list = out_list[:nb_lookup]
    if temp_prefix == "":
        for name in out_list:
            nt.add_name(name, i2a)
    else:
        # split the file with units per processor   
        nb_process = os.cpu_count()
        # prepare at most one bucket per processor 
        bucket_list = []
        todo_list = out_list
        for bucket_id in range(0, nb_process):
            nb_queries = int(len(todo_list)/(nb_process - len(bucket_list)))
            if nb_queries == 0:
                nb_queries = 1
            target = todo_list[-nb_queries:]
            temp_name = temp_prefix + str(bucket_id) + "_ns_data.txt"
            this_bucket = ns_server_lookup_bucket(bucket_id, temp_name, target, i2a)
            bucket_list.append(this_bucket)
            todo_list=todo_list[:-nb_queries]
            if len(todo_list) == 0:
                break
        # launch the parallel buckets
        print("Launching " + str(len(bucket_list)) + " parallel buckets")
        with concurrent.futures.ProcessPoolExecutor(max_workers = nb_process) as executor:
            future_to_bucket = {executor.submit(load_dns_look_up_bucket, bucket):bucket for bucket in bucket_list }
            for future in concurrent.futures.as_completed(future_to_bucket):
                bucket = future_to_bucket[future]
                try:
                    data = future.result()
                    sys.stdout.write(".")
                    sys.stdout.flush()
                except Exception as exc:
                    traceback.print_exc()
                    print('\nBucket %d generated an exception: %s' % (bucket.bucket_id, exc))
                    exit(1)
        print("\nAll buckets completed.")
        # aggregate the results
        for bucket in bucket_list:
            nt.load(bucket.bucket_file_name)
            sys.stdout.write(".")
            sys.stdout.flush()
        print("\nResults loaded from all buckets.")
    print("Processed " + str(len(nt.table)) + " names")
    nt.save(result_file)
# actual main program, can be called by threads, etc.
if __name__ == '__main__':
    main()