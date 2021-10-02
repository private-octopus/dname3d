# testing the million_random class
# example:
# py million_random_test.py ..\data\million.txt 100000

import sys
import traceback
import million_random
import time

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

million_file = sys.argv[1]
already_tested = []
nb_trials_per_round = int(sys.argv[2])

#for x in [ "dict", "set", "list"]:
#    time_trial(x, nb_trials_per_round, million_file)


ref_dict, ref_ranges = million_random.million_dict(million_file,100,10)
print("Reference: " + str(len(ref_dict)) + " names in " + str(ref_ranges) + " ranges.")

mr = million_random.million_random(100, 10)
mr.load(million_file)
trials_required = mr.nb_names()
trials_done = 0
nb_rounds = 0
remains = 0
range_total = []

print("Targeting " + str(mr.nb_names()) + " names in " + str(mr.nb_ranges()) + " ranges.")
for x in range(0, mr.nb_ranges()):
    range_total.append(0)

while trials_done < trials_required:
    try:
        nb_rounds += 1       
        round_start = time.time()
        if len(already_tested) > 0:
            # simulate running a shell scrip N names at a time
            for tested in already_tested:
                mr.set_already_processed(tested)
            if len(mr.already_processed) != len(already_tested):
                print("Error. Found " + str(len(mr.already_processed)) + " processed entries, expected " + str(len(already_tested)) )
                exit(1)
            mr.load(million_file)
            if remains != mr.nb_names():
                print("Error on beginning of round " + str(nb_rounds) + ", " + str(mr.nb_names()) + " names left instead of " + str(remains))
                exit(1)
            load_end = time.time()
            print("now targeting " + str(mr.nb_names()) + " names in " + str(mr.nb_ranges()) + " ranges, loaded in " + str(load_end - round_start))
        pick_start = time.time()
        for trial in range(0, nb_trials_per_round):
            x = mr.random_pick()
            if x == "":
                # current range is empty
                print("Range number " + str(mr.random_range) + " is empty.")
                if not mr.next_random_range():
                    # no other empty range
                    print("Error. All ranges empty after " + str(trials_done) + " trials, but loop did not stop.")
                    exit (1)
                else:
                    x =  mr.random_pick()
                    if x == "":
                        print("Error. Range " + str(mr.random_range) + " is empty, yet was picked.")
                        exit(1)
            trials_done += 1
            range_total[ref_dict[x]] += 1
            already_tested.append(x)
            mr.mark_read(x)
            if not mr.next_random_range():
                # no other empty range
                print("All ranges empty after " + str(trials_done) + " trials.")
                break
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


if mr.nb_names() != 0 or len(already_tested) != trials_required:
    print("Error. After " + str(nb_rounds) + " rounds " + str(mr.nb_names()) + " names remain, found " + str(len(already_tested)) + " instead of " + str(trials_required))
    exit(1)
else:
    print("Success after " + str(nb_rounds) + " rounds and " +  str(trials_done) + " trials.")
exit(0)



