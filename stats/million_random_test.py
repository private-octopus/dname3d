# testing the million_random class
# example:
# py million_random_test.py ..\data\million.txt 100000

import sys
import traceback
import million_random
import time
import zoneparser
import pubsuffix
import dnslook
import zonesampler

def time_trial(x, nb_trials, mfn):
    time_start = time.time()
    mt = million_random.million_time()
    if x == "dict":
        mt.load_dict(mfn)
    elif x == "set":
        mt.load_set(mfn)
    elif x == "list":
        mt.load_list(mfn)
    else:
        print("Cannot do time_trial (" + x + ")")
        return
    time_loaded = time.time()
    print("Trial " + x + " loaded in " + str(time_loaded - time_start))
    time_start_pick = time.time()
    for i in range(0,nb_trials):
        if x == "dict":
            mt.rand_dict()
        elif x == "set":
            mt.rand_set()
        elif x == "list":
            mt.rand_list()
        else:
            break
    time_end_pick = time.time()
    print("Trial " + x + " picked " + str(nb_trials) + " in " + str(time_end_pick - time_start_pick))
    time_start_pick_del = time.time()
    for i in range(0,nb_trials):
        if x == "dict":
            mt.rand_del_dict()
        elif x == "set":
            mt.rand_del_set()
        elif x == "list":
            mt.rand_del_list()
        else:
            break
    time_end_pick_del = time.time()
    print("Trial " + x + " picked and deleted " + str(nb_trials) + " in " + str(time_end_pick_del - time_start_pick_del))

def get_prefix(name,ps):
    x,is_suffix = ps.suffix(name)
    if x == "" or not is_suffix:
        np = name.split(".")
        l = len(np)
        while l >= 2 and len(np[l-1]) == 0:
            l -= 1
        if l >= 2:
            x = (np[l-2] + "." + np[l-1])
        if x == "":
            print("Escaped " + name + " to " + x)
    return x

#main
million_file = sys.argv[1]
zone_sample_file = sys.argv[2]
already_tested = []
nb_trials_per_round = int(sys.argv[3])
public_suffix_file = sys.argv[4]
#for x in [ "dict", "set", "list"]:
#    time_trial(x, nb_trials_per_round, million_file)
ps = pubsuffix.public_suffix()
if not ps.load_file(public_suffix_file):
    print("Could not load the suffixes")
    exit(1)

print("Test sanity of " + dnslook.sanitize("1234567890._-.abc\032-DeF-xyZ"))

#ref_dict, ref_ranges = million_random.million_dict(million_file,100,10)
#print("Reference: " + str(len(ref_dict)) + " names in " + str(ref_ranges) + " ranges.")

mr = million_random.million_random(100, 10)
mr.load(million_file)

# check duplicate in zone file:
zone_check = set()
nb_zone_dup = 0
nb_zone_total = 0
for line in open(zone_sample_file,"rt"):
    zs = zonesampler.one_zone_sample("", "")
    zs.from_json(line)
    if zs.domain.endswith("."):
        zs.domain = zs.domain[0:-1]
    if nb_zone_total < 10:
        print("Zone: <" + zs.domain + ">")
    nb_zone_total += 1
    zone_check.add(zs.domain)
    if zs.domain in mr.already_processed:
        if nb_zone_dup < 10:
            print("Already in millions: " + zs.domain)
        nb_zone_dup += 1
       
print("Out of " + str(nb_zone_total) + " in " + zone_sample_file + ", " + str(nb_zone_dup) + " duplicates.")

# Add the names from the zone sample file
mr.load_zone_sample(zone_sample_file)

trials_required = mr.nb_names()
if len(sys.argv) > 5:
    trials_required = int(sys.argv[5])
trials_done = 0
nb_rounds = 0
remains = 0
range_total = []
result_list = []

print("Targeting " + str(trials_required) + " names in " + str(mr.nb_ranges()) + " ranges out of " + str(mr.nb_names()))

for x in range(0, mr.nb_ranges()):
    range_total.append(0)

while trials_done < trials_required:
    # simulate running a shell script N names at a time
    try:
        nb_rounds += 1       
        round_start = time.time()
        if len(already_tested) > 0:
            for tested in already_tested:
                mr.set_already_processed(tested)
            if len(mr.already_processed) != len(already_tested):
                print("Error. Found " + str(len(mr.already_processed)) + " processed entries, expected " + str(len(already_tested)) )
                exit(1)
            mr.load(million_file)
            mr.load_zone_sample(zone_sample_file)
            if remains != mr.nb_names():
                print("Error on beginning of round " + str(nb_rounds) + ", " + str(mr.nb_names()) + " names left instead of " + str(remains))
                exit(1)
        load_end = time.time()
        for a in range(0, len(mr.range_names)):
            print("Range " + str(a) + ", " + str(len(mr.range_names[a])) + " names")
        for r in range(0, len(mr.range_list)):
            print("Range[" + str(r) + "] = " + str(mr.range_list[r]))
        print("now targeting " + str(mr.nb_names()) + " names in " + str(mr.nb_ranges()) + " ranges, loaded in " + str(load_end - round_start))
        pick_start = time.time()
        for trial in range(0, nb_trials_per_round):
            if mr.nb_names() == 0:
                # no other empty range
                print("All ranges empty after " + str(trials_done) + " trials.")
                break
            x = mr.random_pick()
            if x.domain == "":
                print("Error after " + str(trial) + " trials, empty response.")
                print("Names remaining: " + str(mr.nb_names()))
                break
            trials_done += 1
            already_tested.append(x.domain)
            mr.mark_read(x.domain)
            result_list.append(x)
            if x.million_range < 0 or x.million_range >= len(range_total):
                print("Unexpected range = " + str(x.million_range) + " for " + x.domain)
                exit(1)
            else:
                range_total[x.million_range] += 1
        remains = mr.nb_names()
        round_end = time.time()
        print("After " + str(nb_rounds) + " rounds in " + str(round_end - pick_start) + ", " + str(remains) + " names remain.")
        s = ""
        for r in range_total:
            if s != "":
                s += ", "
            s += str(r)
        print("Found in ranges: " + s)
    except Exception as e:
        traceback.print_exc()
        print("Cannot perform round <" + str(nb_rounds) + ">\nException: " + str(e))
        print("Giving up");
        exit(1)
    if mr.nb_names() == 0:
        break
        
# Double check that the distribution of names is maintained after parsing as zones.
md,nmd = million_random.million_dict(million_file, 100, 10)
print("\nLoaded " + str(len(md)) + " millions.")
total_per_level = [0,0,0,0,0,0]
nb_direct_find = 0
nb_not_found = 0
nb_rank_not_match = 0
check_for_dup = set()
nb_duplicates = 0
for target in result_list:
    y = target.domain
    million_range = 5
    if y in md:
        million_range = md[y]
        nb_direct_find += 1
    elif y in zone_check:
        million_range = 5
        nb_direct_find += 1
    else:
        if nb_not_found < 10:
            print("Not found: " + y)
        nb_not_found += 1

    if million_range != target.million_range:
        if nb_rank_not_match < 10:
            print("Target(" + target.domain + ", " + str(target.million_rank)  + ", " + str(target.million_range) + ") ranked as: " + str(million_range))
        nb_rank_not_match += 1
    if target.domain in check_for_dup:
        if nb_duplicates < 10:
            print("Target(" + target.domain + ", " + str(target.million_rank) + ", " + str(target.million_range) + ") already found!")
        nb_duplicates += 1
    else:
        check_for_dup.add(target.domain)
    total_per_level[million_range] += 1

print("Found direct: " + str(nb_direct_find) + ", not: " + str(nb_not_found))
print("Rank mismatch: " + str(nb_rank_not_match) + ", duplicates: " + str(nb_duplicates))
for i in range(0,len(total_per_level)):
    print("Level " + str(i) + ": " + str(total_per_level[i]))

if len(sys.argv) > 5:
    with open(sys.argv[5],"wt") as tf:
        for name in already_tested:
            tf.write(name + "\n")

if len(already_tested) != trials_required:
    print("Error. After " + str(nb_rounds) + " rounds " + str(mr.nb_names()) + " names remain, found " + str(len(already_tested)) + " instead of " + str(trials_required))
    exit(1)
else:
    print("Success after " + str(nb_rounds) + " rounds and " +  str(trials_done) + " trials.")
exit(0)



