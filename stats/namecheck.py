#!/usr/bin/python
# coding=utf-8
#
# Merging of name files into a single list and extraction of the common prefixes.
# Since the number of files to merge can be quite large, we spread the processing
# on all available cores.

import sys
import prefixlist
import traceback
import time
import concurrent.futures
import os

def usage(argv_0):
    print("Usage:\n" + argv_0 + " depth prefix_file name_file*")
    print("    depth:        maximum prefix length to be tabulated.")
    print("    prefix_file:  file in which prefixes will be collected.")
    print("    temp:         prefix for temporary files.")
    print("    name_file*:   at least one file containing name lists.")

class name_bucket:
    def __init__(self, bucket_id, depth, result_file_name, input_files):
        self.bucket_id = bucket_id
        self.input_files = input_files
        self.result_file_name = result_file_name
        self.prefix_list = prefixlist.prefixlist(depth)

    def load(self):
        try:
            for input_file in self.input_files:
                self.prefix_list.load_logfile(input_file)
        except:
            traceback.print_exc()
            print("Abandon bucket " + str(self.bucket_id))
        return True

    def save(self):
        self.prefix_list.write_file(self.result_file_name)

def load_name_bucket(bucket):
    bucket.load()
    bucket.save()

# main loop
def main():
    depth = 0
    if len(sys.argv) >= 5:
        try:
            depth = int(sys.argv[1])
        except:
            depth = 0
        if depth == 0:
            print("Incorrect depth value: " + argv[1])
    if depth == 0:
        usage(sys.argv[0])
        exit(1)
    result_file = sys.argv[2]
    temp_prefix = sys.argv[3]
    nb_process = os.cpu_count()
    nb_files = len(sys.argv) - 4
    files = sys.argv[4:len(sys.argv)]
    bucket_list = []
    print("Aiming for " + str(nb_process) + " processes")

    # prepare a set of sublists and result names for each process.
    process_left = nb_process
    if sys.argv[3] == "-":
        process_left = 1
    files_left = nb_files
    bucket_id = 0
    while files_left > 0:
        nb_file_this_process = int(files_left / process_left)
        if nb_file_this_process == 0:
            nb_file_this_process = 1
        this_bucket_files = files[files_left - nb_file_this_process : files_left]
        bucket_id += 1
        temp_name = temp_prefix + str(bucket_id) + ".csv"
        this_bucket = name_bucket(bucket_id, depth + 1, temp_name, this_bucket_files)
        bucket_list.append(this_bucket)
        files_left -= nb_file_this_process
        process_left -= 1

    nb_process = min(nb_process, len(bucket_list))
    print("Will use " + str(nb_process) + " processes, " + str(len(bucket_list)) + " buckets")
    
    start_time = time.time()
    if len(bucket_list) > 1:
        with concurrent.futures.ProcessPoolExecutor(max_workers = nb_process) as executor:
            future_to_bucket = {executor.submit(load_name_bucket, bucket):bucket for bucket in bucket_list }
            for future in concurrent.futures.as_completed(future_to_bucket):
                bucket = future_to_bucket[future]
                try:
                    data = future.result()
                except Exception as exc:
                    print('Bucket %d generated an exception: %s' % (bucket.bucket_id, exc))
        bucket_time = time.time()
        print("Threads took " + str(bucket_time - start_time))
        prefix_list = prefixlist.prefixlist(depth)
        for bucket in bucket_list:
            prefix_list.load_prefix_file(bucket.result_file_name)
        summary_time = time.time()
        print("Summary took " + str(summary_time - start_time))
        prefix_list.write_file(result_file)
    else:
        bucket_list[0].result_file_name = result_file
        bucket_list[0].depth = depth
        load_name_bucket(bucket_list[0])
        print("Loaded a single bucket into " + result_file)

# actual main program, can be called by threads, etc.

if __name__ == '__main__':
    main()