#!/usr/bin/python
# coding=utf-8
#
# Parse the names file and extract statistics similar to sum3,
# but also including the dga13 category.

import traceback
import sys
#!/usr/bin/python
# coding=utf-8
#
# Merge a number of prefix entries into a single prefix summary.


import sys
import prefixlist
import traceback
import time
import os

prefix_out = sys.argv[1]
files = sys.argv[2:]

ps = prefixlist.prefix_summary()

for file_name in files:
    ps.parse_prefixes(file_name)

ps.save_prefix_summary(prefix_out, 0)


