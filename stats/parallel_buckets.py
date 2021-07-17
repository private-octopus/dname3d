#!/usr/bin/python
# coding=utf-8
#
# Generic handling of input parallelism

import concurrent.futures
import os
import traceback

def load_bucket(bucket):
    bucket.load()
    bucket.save()

def init_buckets(bucket_class, max_buckets, files, params):
    bucket_list=[]
    buckets_left = max_buckets
    files_left = len(files)
    bucket_id = 0
    while files_left > 0:
        bucket = bucket_class()
        nb_file_this_bucket = int(files_left / buckets_left)
        if nb_file_this_bucket == 0:
            nb_file_this_bucket = 1
        bucket.bucket_id = bucket_id
        bucket.input_files = files[files_left - nb_file_this_bucket : files_left]
        bucket.complete_init(params)
        bucket_list.append(bucket)
        bucket_id += 1
        files_left -= nb_file_this_bucket
        buckets_left -= 1
    return bucket_list

def run_buckets(bucket_list):
    with concurrent.futures.ProcessPoolExecutor(max_workers = nb_process) as executor:
        future_to_bucket = {executor.submit(load_bucket, bucket):bucket for bucket in bucket_list }
        for future in concurrent.futures.as_completed(future_to_bucket):
            bucket = future_to_bucket[future]
            try:
                data = future.result()
                sys.stdout.write(".")
                sys.stdout.flush()
            except Exception as exc:
                traceback.print_exc()
                print('\nBucket %d generated an exception: %s' % (bucket.bucket_id, exc))
