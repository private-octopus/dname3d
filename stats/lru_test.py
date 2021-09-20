# Unit test of the LRU module
#
# Expect this test to work:
#
# py .\lru_test.py 5 ..\data\lru_test_in.txt ..\data\lru_test_ref.csv ..\tmp\lru_test_res.csv

import sys
import zoneparser
import compare_file_test
import traceback
import random

class lru_test_entry:
    def __init__(self):
        self.hit_count = 0
        self.last_hit = -1
        self.first_hit = -1

def lru_list(lru):
    s = ""
    old = ""
    key = lru.lru_first
    while key != "":
        if lru.table[key].lru_previous != old:
            s += "<" + lru.table[key].lru_previous + "> "
        s += key + ", "
        old = key
        key = lru.table[key].lru_next
    if lru.lru_last != old:
        s += "<" + lru.lru_last + "> "
    return s

# main program

limit = int(sys.argv[1])
test_in = sys.argv[2]
test_ref = sys.argv[3]
test_out = sys.argv[4]

lru = zoneparser.lru_list(limit,lru_test_entry)
print("LRU list max " + str(lru.target_number) + ", starts with " + str(len(lru.table)))
rank = 0
for line in open(test_in, "rt"):
    try:
        rank += 1
        key = line.strip()
        if lru.add(key):
            lru.table[key].data.hit_count += 1
            lru.table[key].data.last_hit = rank
            if lru.table[key].data.first_hit < 0:
                lru.table[key].data.first_hit = rank
            print("Rank: " + str(rank) + ", " + key + ", lru: " + lru_list(lru))
        else:
            break
    except:
        traceback.print_exc()
        print("Could not load " + test_in + " line " + str(rank) + ": " + key)
        exit(1)
        
f = open(test_out,"wt")
f.write("Key,hit-count,first_rank,last_rank\n")
nb_saved = 0
key = lru.lru_first
while key != "":
    f.write(key + "," + str(lru.table[key].data.hit_count) + "," +  str(lru.table[key].data.first_hit) + "," +  str(lru.table[key].data.last_hit) + "\n")
    key = lru.table[key].lru_next
    nb_saved += 1
f.close()
print("saved " + str(nb_saved) + " entries.")
# Compare outouts to expected result
if not compare_file_test.compare_files(test_out, test_ref):
    exit(1)

fuzz_list = []
for line in open(test_in, "rt"):
    fuzz_list.append(line.strip())
for x in range(0,10000000):
    key = random.choice(fuzz_list)
    if not lru.add(key):
        print("Fuzz break " + str(x) + ": " + key)
        exit(1)
print("Fuzz complete.")
