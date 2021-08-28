# test of suffix sample
#
# Expected to run the test as:
# py .\suffix_sample_test.py 2 ..\data\suffix_sample_list.txt ..\tmp\suffix_samples.csv ..\data\suffix_samples_ref.csv ..\data\suffix_test_names.csv ..\data\suffix_test_names_2.csv

import sys
import suffix_sample
import compare_file_test
import os

# main

nb_samples = int(sys.argv[1])
test_suffixes = sys.argv[2]
sample_out = sys.argv[3]
sample_ref = sys.argv[4]
files_in = sys.argv[5:]

ssl = suffix_sample.suffix_sample_list(nb_samples)

# load the target suffixes
for line in open(test_suffixes, "rt"):
    ssl.add_suffix(line.strip())

# process the input files
for log_file in files_in:
    ssl.add_log_file(log_file)

# save the test results
ssl.save(sample_out)

# Compare outouts to expected result
if not compare_file_test.compare_files(sample_out, sample_ref):
    exit(1)
else:
    exit(0)


