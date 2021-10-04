# Unit test of the zoneparser module
#
# Expect these tests to work:
#
# ...

import sys
import zoneparser
import compare_file_test
import pubsuffix
import traceback
import time
import concurrent.futures
import os


class zone_bucket:
    def __init__(self, bucket_id, result_file_name, zone_file, p_start, p_end, zp):
        self.bucket_id = bucket_id
        self.zone_file = zone_file
        self.result_file_name = result_file_name
        self.p_start = p_start
        self.p_end = p_end
        self.zp = zp

    def load(self):
        try:
            self.zp.add_zone_file(self.zone_file,  p_start=self.p_start, p_end=self.p_end)
        except:
            traceback.print_exc()
            print("Abandon bucket " + str(self.bucket_id))
        return True

    def save(self):
        self.zp.save(self.result_file_name)

def load_zone_bucket(bucket):
    bucket.load()
    bucket.save()

# main program

def main():
    if len(sys.argv) < 6:
        print("Usage: " + sys.argv[0] + " out_file zone_file ps_file dup_file million_file [temp_prefix]")
        exit(1)

    ns_out = sys.argv[1]
    zone_file = sys.argv[2]
    ps_file = sys.argv[3]
    dup_file = sys.argv[4]
    million_file = sys.argv[5]
    temp_prefix = ""
    if len(sys.argv) == 7:
        temp_prefix = sys.argv[6]

    print("Parsing " + zone_file + ", results in " + ns_out)
    start_time = time.time()
    ps = pubsuffix.public_suffix()
    ps.load_file(ps_file)
    print("found " + str(len(ps.table)) + " public suffixes.")

    zp = zoneparser.zone_parser2(ps)
    zp.load_dups(dup_file)
    zp.load_million(million_file)
    print("found " + str(len(zp.millions)) + " million-hosts prefixes in " + million_file)
    ready_time = time.time()
    print("Prepared in " + str(ready_time - start_time))
    if temp_prefix == "":
        try:
            zp.add_zone_file(zone_file)
        except:
            traceback.print_exc()
            print("Could not load " + zone_file) 
            exit(1)
        zone_done_time = time.time()
        print("Loaded " + str(zp.name_count) + " names, found " + str(len(zp.sf_dict)) + " services in " + str(zone_done_time - ready_time))
    else:
        # split the file with units per processor   
        nb_process = os.cpu_count()
        file_part = zoneparser.compute_file_partitions(zone_file,nb_process)
        # prepare a bucket per processor 
        bucket_list = []
        for bucket_id in range(0,len(file_part)-1):
            temp_name = temp_prefix + str(bucket_id) + "_zone.csv"
            this_bucket = zone_bucket(bucket_id, temp_name, zone_file, file_part[bucket_id], file_part[bucket_id+1], zp)
            bucket_list.append(this_bucket)
        # run multiple parsing in parallel
        with concurrent.futures.ProcessPoolExecutor(max_workers = nb_process) as executor:
            future_to_bucket = {executor.submit(load_zone_bucket, bucket):bucket for bucket in bucket_list }
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
        bucket_time = time.time()
        print("\nThreads took " + str(bucket_time - ready_time))
        # aggregate the results
        for bucket in bucket_list:
            zp.load_partial_result(bucket.result_file_name)
            sys.stdout.write(".")
            sys.stdout.flush()
        zone_done_time = time.time()
        print("\nSummary took " + str(zone_done_time - bucket_time))
    # save
    zp.save(ns_out)
    final_time = time.time()
    print("Save took " + str(final_time - zone_done_time))
    print("Total took " + str(final_time - start_time))

# actual main program, can be called by threads, etc.
if __name__ == '__main__':
    main()
