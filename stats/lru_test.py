# Unit test of the zoneparser module
#
# Expect these tests to work:
#
# ...

import sys
import zoneparser
import compare_file_test
import traceback

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
test_out = sys.argv[3]

lru = zoneparser.lru_list(limit,lru_test_entry)
print("LRU list max " + str(lru.target_number) + ", starts with " + str(len(lru.table)))
rank = 0
for line in open(test_in, "rt"):
    try:
        rank += 1
        key = line.strip()
        if lru.add(line.strip()):
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
        break
        
f = open(test_out,"wt")
f.write("Key,hit-count,first_rank,last_rank\n")
key = lru.lru_first
while key != "":
    f.write(key + "," + str(lru.table[key].data.hit_count) + "," +  str(lru.table[key].data.first_hit) + "," +  str(lru.table[key].data.last_hit) + "\n")
    key = lru.table[key].lru_next
f.close()

