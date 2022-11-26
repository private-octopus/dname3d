# Do suffix reports


import sys
import suffixes
import concurrent.futures
import traceback
import time
import os

def usage(argv_0):
    print("Usage:\n" + argv_0 + "top_n report_file city_prefix date_prefix file_list")
    print("    report_file:  file in which top_n results will be collected.")
    print("    city_prefix:  prefix under for saving city files.")
    print("    date_prefix:  prefix under for saving date files.")
    print("    file_list:    contains list of files to be read.")
    exit(1)

class suffix_bucket:
    def __init__(self, bucket_id, hll_k, max_suffix_parts, city, date, input_files, result_file_name):
        self.bucket_id = bucket_id
        self.hll_k = hll_k
        self.max_suffix_parts = max_suffix_parts
        self.input_files = input_files
        self.city = city
        self.date = date
        self.result_file_name = result_file_name

    def process(self):
        try:
            suffixes.suffix_report.compute_city_or_date_report(self.hll_k, self.max_suffix_parts, \
                 self.city, self.date, self.input_files, self.result_file_name)
        except:
            traceback.print_exc()
            print("Abandon bucket " + str(self.bucket_id))
        return True

def load_suffix_bucket(bucket):
    bucket.process()

# main
def main():
    top_n = 0
    if len(sys.argv) != 6:
        usage(sys.argv[0])
    else:
        try:
            top_n = int(sys.argv[1])
        except:
            top_n = 0

    if top_n <= 0:
        print("Expected the top_n number, got " + sys.argv[1])
        usage(sys.argv[0])
    print("There will be " + sys.argv[1] + " top suffixes.")
     
    report_file = sys.argv[2]
    city_prefix = sys.argv[3]
    date_prefix = sys.argv[4]
    file_names = sys.argv[5]
    in_files = []
    for line in open(file_names , "rt", encoding="utf-8"):
        in_files.append(line.strip())

    print("There are " + str(len(in_files)) + " input files")
    if len(in_files) == 0:
        usage(sys.argv[0])

    # Create the report object, and then document the list of files.
    sr = suffixes.suffix_report(4,3,top_n)
    sr.set_city_date_lists(in_files)

    # Create a compute bucket for each city and each date
    bucket_list = []
    bucket_id = 0
    for date in sr.date_list:
        result_file = date_prefix + date + ".csv"
        bucket_list.append(suffix_bucket(bucket_id, sr.hll_k, \
            sr.max_suffix_parts, "", date, in_files, result_file))
    print("Added " + str(len(sr.date_list)) + " dates")
    for city in sr.city_list:
        result_file = city_prefix + city + ".csv"
        bucket_list.append(suffix_bucket(bucket_id, sr.hll_k, \
            sr.max_suffix_parts, city, "", in_files, result_file))
    print("Added " + str(len(sr.city_list)) + " cities")
    print("Total: " + str(len(bucket_list)) + " buckets")

    # Set the maximum number of processes to the CPU number
    nb_process = os.cpu_count()

    # process the buckets, create result files for each data and city
    start_time = time.time()
    with concurrent.futures.ProcessPoolExecutor(max_workers = nb_process) as executor:
        future_to_bucket = {executor.submit(load_suffix_bucket, bucket):bucket for bucket in bucket_list }
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

    # Now load each of the required results
    # todo: should write a parse version that only loads the required suffixes
    for bucket in bucket_list:
        if bucket.city != "":
            sr.city_list[bucket.city].parse(bucket.result_file_name)
        else:
            sr.date_list[bucket.date].parse(bucket.result_file_name)
        sys.stdout.write(".")
        sys.stdout.flush()
    summary_time = time.time()
    print("\nSummary took " + str(summary_time - bucket_time))

    # finally save the report
    sr.get_top_domains()
    print("Loaded " + str(len(sr.top_list.suffixes)) + " top domains.")
    topload_time = time.time()
    print("\nTop domains took " + str(topload_time - summary_time))
    sr.save_top_details(report_file)
    report_time = time.time()
    print("\nReport took " + str(report_time - topload_time))
    print("\nTotal took " + str(report_time - start_time))

# actual main program, can be called by threads, etc.

if __name__ == '__main__':
    main()