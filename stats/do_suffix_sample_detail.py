# obtain suffix sample
#
# Expected to run the test as:
# python3 .\suffix_sample_detail_test.py nb  /data/ITHI/results-name/WEST/ ~/tmp/suffix_details.csv  ~/tmp/suffix_report_ref.csv

import sys
import suffix_sample
import compare_file_test
import os

# main

nb_samples = int(sys.argv[1])
dir_prefix = sys.argv[2]
result_file = sys.argv[3]
details = sys.argv[4]

sds = suffix_sample.suffix_details_sample(nb_samples, dir_prefix)

# load details file
sds.load_detail_file(details)

# get the samples from the details file
sds.get_samples()

# save the result files
sds.sample.save(result_file)

# Compare outouts to expected result
if not compare_file_test.compare_files(audited, audited_ref):
    exit(1)
else:
    exit(0)


