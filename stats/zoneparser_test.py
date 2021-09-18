# Unit test of the zoneparser module
#
# Expect these tests to work:
#
# ...

import sys
import zoneparser
import compare_file_test
import pubsuffix

# main program

ns_out = sys.argv[1]
ps_file = sys.argv[2]
zones = sys.argv[3:]

ps = pubsuffix.public_suffix()
ps.load_file(ps_file)
print("found " + str(len(ps.table)) + " public suffixes.")

zp = zoneparser.zone_parser(ps)

for zone_file in zones:
    try:
        zp.add_zone_file(zone_file)
    except:
        traceback.print_exc()
        print("Could not load " + zone_file) 

zp.save(ns_out)


