# test of suffix sample
#
# Expected to run the test as:
# py .\suffix_sample_detail_test.py 5 ..\data\ ..\tmp\audit_details.csv ..\data\audit_details_ref.csv ..\data\suffix_report_ref.csv

import sys
import suffix_sample
import compare_file_test
import os

# main

nb_samples = int(sys.argv[1])
dir_prefix = sys.argv[2]
audited = sys.argv[3]
audited_ref = sys.argv[4]
details = sys.argv[5]

sds = suffix_sample.suffix_details_sample(nb_samples, dir_prefix)

# load details file
sds.load_detail_file(details)

# get the samples from the details file
sds.get_samples()

#audit
sds.audit_details(audited)


# Compare outouts to expected result
if not compare_file_test.compare_files(audited, audited_ref):
    exit(1)
else:
    exit(0)


