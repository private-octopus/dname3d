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

# main program

if len(sys.argv) < 6:
    print("Usage: " + sys.argv[0] + "ns_out ps_file dup_file million_file zone_file*")
    exit(1)

ns_out = sys.argv[1]
ps_file = sys.argv[2]
dup_file = sys.argv[3]
million_file = sys.argv[4]
zones = sys.argv[5:]

print("Parsing " + str(len(zones)) + ", results in " + ns_out)

ps = pubsuffix.public_suffix()
ps.load_file(ps_file)
print("found " + str(len(ps.table)) + " public suffixes.")

zp = zoneparser.zone_parser2(ps)
zp.load_dups(dup_file)
zp.load_million(million_file)
print("found " + str(len(zp.millions)) + " million-hosts prefixes in " + million_file)

for zone_file in zones:
    zone_start = time.time()
    try:
        zp.add_zone_file(zone_file)
    except:
        traceback.print_exc()
        print("Could not load " + zone_file) 
    zone_done = time.time()
    print("Loaded " + str(zp.name_count) + " names, found " + str(len(zp.sf_dict)) + " services in " + str(zone_done - zone_start))

zp.save(ns_out)


