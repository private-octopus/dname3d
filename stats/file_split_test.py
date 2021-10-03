# Testing the split file functions
import sys
import os
import traceback
import zoneparser
import compare_file_test
import pubsuffix
import time

def parse_zone_file(ps, dup_file, million_file, zone_file, zone_res, p_start, p_end):
    zp = zoneparser.zone_parser2(ps)
    zp.load_dups(dup_file)
    zp.load_million(million_file)
    zone_start = time.time()
    try:
        zp.add_zone_file(zone_file,  p_start=p_start, p_end=p_end)
    except:
        traceback.print_exc()
        print("Could not load " + zone_file)
        exit(1)
    zone_done = time.time()
    print(zone_file + "[ " + str(p_start) + " : " + str(p_end) + "] : loaded " + str(zp.name_count) + " names, found " + str(len(zp.sf_dict)) + " services in " + str(zone_done - zone_start))
    zp.save(zone_res)


#  main
file_name = sys.argv[1]
copy_name = sys.argv[2]
ps_file = sys.argv[3]
dup_file = sys.argv[4]
million_file = sys.argv[5]
zone_one_shot = sys.argv[6]
zone_four_shots = sys.argv[7]
zone_temp = sys.argv[8]
file_size = 0

try:
    file_part = zoneparser.compute_file_partitions(file_name,4)
    for x in range(0,4):
        print("Range [" + str(x) + "]: [ " + str(file_part[x]) + " : " + str(file_part[x+1]) + "]")
except Exception as e:
    traceback.print_exc()
    print("Cannot get size of file <" + file_name  + ">\nException: " + str(e))
    print("Giving up");
    exit(1)


# Create a copy by reading the 4 ranges
f_out = open(copy_name, "wt", encoding="utf-8")
for x in range(0,4):
    # open file
    file = open(file_name, "rt", encoding="utf-8")
 
    # get the cursor positioned at partition
    file.seek(file_part[x])
    file_pos = file_part[x]
    print("Partition " + str(x) + " starts at " + str(file_pos))
    for line in file:
        file_pos += len(line)
        f_out.write(line)
        if file_pos >= file_part[x+1]:
            break
    if file_pos != file_part[x+1]:
        print("Error: partition " + str(x) + " ends at " + str(file_pos) + " instead of " +  str(file_part[x+1]))
        exit(1)
    print("Partition " + str(x) + " ends at " + str(file_pos))
    file.close()
f_out.close()

# Compare in and out

if not compare_file_test.compare_files(copy_name, file_name):
    print("Error, " + copy_name + " and " + file_name + " differ.")
    exit(1)

print(copy_name + " and " + file_name + " match.")

# Verify that parsing by segments gets the same result as parsing the whole file

ps = pubsuffix.public_suffix()
ps.load_file(ps_file)
print("found " + str(len(ps.table)) + " public suffixes.")

# Create a reference file
parse_zone_file(ps, dup_file, million_file, file_name, zone_one_shot, 0, 0);

# create a merging context
zp = zoneparser.zone_parser2(ps)
zp.load_dups(dup_file)
zp.load_million(million_file)

# parse and merge each partition
for x in range(0,4):
    parse_zone_file(ps, dup_file, million_file, file_name, zone_temp, file_part[x], file_part[x+1]);
    if not zp.load_partial_result(zone_temp):
        print("Could not load " + zone_temp)
        exit(1)

# save merged file and verify
zp.save(zone_four_shots)

if not compare_file_test.compare_files(zone_four_shots, zone_one_shot):
    print("Error, " + zone_four_shots + " and " + zone_one_shot + " differ.")
    exit(1)

print(zone_four_shots + " and " + zone_one_shot + " match.")


    
