#!/usr/bin/python
# coding=utf-8
#
# Extract name statistics from a set of files.
# Since the number of files to merge can be quite large, we spread the processing
# on all available cores.


import sys
import namestats
import traceback
import time
import concurrent.futures
import os

dga_subnet_list = [
    ]

def usage(argv_0):
    print("Usage:\n" + argv_0 + " result_file suffix_file temp dga_subnets name_file*")
    print("    result_file:  file in which results will be collected.")
    print("    suffix_file:  file in which suffixes are collected.")
    print("    temp:         prefix for temporary files (or \"-\" if only 1 process).")
    print("    dga_subnets:  text file containing list of dga13 subnets."
    print("    name_file*:   at least one file containing name lists.")

class name_bucket:
    def __init__(self, bucket_id, result_file_name, suffix_file_name, dga_subnets, input_files):
        self.bucket_id = bucket_id
        self.input_files = input_files
        self.result_file_name = result_file_name
        self.suffix_file_name = suffix_file_name
        self.stats = namestats.namestats(dga_subnets)

    def load(self):
        try:
            for input_file in self.input_files:
                self.stats.load_logfile(input_file)
        except:
            traceback.print_exc()
            print("Abandon bucket " + str(self.bucket_id))
        return True

    def save(self):
        self.stats.export_result_file(self.result_file_name)
        self.stats.export_suffix_file(self.suffix_file_name)

def load_name_bucket(bucket):
    bucket.load()
    bucket.save()

# main loop
def main():
    if len(sys.argv) < 6:
        usage(sys.argv[0])
        exit(1)
    result_file = sys.argv[1]
    suffix_file_name = sys.argv[2]
    temp_prefix = sys.argv[3]
    dga_subnets_file = sys.argv[4]
    nb_process = os.cpu_count()
    files = sys.argv[5:len(sys.argv)]
    nb_files = len(files)

    if dga_subnets_file != "-":
        dga_subnets = namestats.subnet_dict_from_file(dga_subnets_file)
    else:
        dga_subnets = dict()

    bucket_list = []
    print("Aiming for " + str(nb_process) + " processes")

    # prepare a set of sublists and result names for each process.
    process_left = nb_process
    if temp_prefix == "-":
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
        temp_suffix = temp_prefix + str(bucket_id) + ".sfx"
        this_bucket = name_bucket(bucket_id, temp_name, temp_suffix, dga_subnets, this_bucket_files)
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
                    sys.stdout.write(".")
                    sys.stdout.flush()
                except Exception as exc:
                    traceback.print_exc()
                    print('\nBucket %d generated an exception: %s' % (bucket.bucket_id, exc))
        bucket_time = time.time()
        print("\nThreads took " + str(bucket_time - start_time))
        stats = namestats.namestats()
        for bucket in bucket_list:
            stats.import_result_file(bucket.result_file_name)
            sys.stdout.write(".")
            sys.stdout.flush()
        summary_time = time.time()
        print("\nSummary took " + str(summary_time - start_time))
        stats.final_dga()
        stats.export_result_file(result_file)
    else:
        bucket_list[0].result_file_name = result_file
        bucket_list[0].load()
        bucket_list[0].stats.final_dga()
        bucket_list[0].save()
        print("Loaded a single bucket into " + result_file)

# actual main program, can be called by threads, etc.

if __name__ == '__main__':
    main()