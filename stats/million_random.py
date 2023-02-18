# Manage the list of a million names used for the random trials 
# Trials are made by categories, 0 to 99, 100 to 999, etc.

import random
import traceback
import time

class million_target:
    def __init__(self, domain, million_rank, million_range):
        self.domain = domain
        self.million_rank = million_rank
        self.million_range = million_range

def million_dict(mfn, first_range, log_factor):
    million_rank = 0
    current_range = 0
    range_start = 0
    range_end = first_range
    million = dict()
    try: 
        for line in open(mfn, "rt", encoding="utf-8"):
            if million_rank >= range_end:
                current_range += 1
                range_start = range_end
                range_end *= log_factor
            mn = line.strip()
            million[mn] = current_range
            million_rank += 1
    except Exception as e:
        traceback.print_exc()
        print("Cannot read file <" + mfn  + ">\nException: " + str(e))
        print("Giving up");
        exit(1)
    return million, current_range + 1

class million_random(object):
    def __init__(self, log_first, log_val):
        self.log_first = log_first
        self.log_val = log_val
        self.already_processed = set()
        self.names_count = 0
        self.range_names = []
        self.range_list = []
        self.nb_collisions = 0

    def load(self, mfn):
        million_rank = 0
        current_range = 0
        range_start = 0
        range_end = self.log_first
        self.range_names = []
        self.range_list = [ 0 ]
        self.range_names.append([])
        self.names_count = 0
        self.nb_collisions = 0
        try: 
            for line in open(mfn, "rt", encoding="utf-8"):
                if million_rank >= range_end:
                    current_range += 1
                    range_start = range_end
                    range_end *= self.log_val
                    self.range_list.append(current_range)
                    self.range_names.append([])
                name = line.strip()
                if not name in self.already_processed:
                    self.range_names[current_range].append(million_target(name, million_rank, current_range))
                    self.names_count += 1
                million_rank += 1
        except Exception as e:
            traceback.print_exc()
            print("Cannot read file <" + mfn  + ">\nException: " + str(e))
            print("Giving up");
            exit(1)
        # Clean up the list of ranges
        popped = True
        while popped:
            popped = False
            for x in range(0, len(self.range_list)):
                a = self.range_list[x]
                l = len(self.range_names[a])
                if l == 0:
                    self.range_list.pop(x)
                    popped = True
                    break
        

    def set_already_processed(self, mn):
        self.already_processed.add(mn)

    def random_pick(self):
        while len(self.range_list) > 0:
            x = random.randrange(0, len(self.range_list))
            a = self.range_list[x]
            l = len(self.range_names[a])
            if l == 0:
                print("Range names " + str(a) + " is empty")
                self.range_list.pop(x)
            else:
                r = random.randrange(0,l)
                target = self.range_names[a][r]
                if target.domain in self.already_processed:
                    self.nb_collisions += 1
                    self.range_names[a].pop(r)
                else:
                    return target
        print("Error: trying to get pick from empty list!")
        return million_target("", 0, 0)

    def mark_read(self, name):
        if not name in self.already_processed:
            self.already_processed.add(name)
            self.names_count -= 1

    def nb_ranges(self):
        return len(self.range_names)

    def nb_names(self):
        return self.names_count

class million_time(object):
    def __init__(self):
        self.mdict = dict()
        self.mset = set()
        self.mlist = []

    def load_dict(self, mfn):
        self.mdict = dict()
        try: 
            for line in open(mfn, "rt", encoding="utf-8"):
                mn = line.strip()
                self.mdict[mn] = 0
        except Exception as e:
            traceback.print_exc()
            print("Cannot read file <" + mfn  + ">\nException: " + str(e))
            print("Giving up");
            exit(1)

    def load_set(self, mfn):
        self.mset = set()
        try: 
            for line in open(mfn, "rt", encoding="utf-8"):
                mn = line.strip()
                self.mset.add(mn)
        except Exception as e:
            traceback.print_exc()
            print("Cannot read file <" + mfn  + ">\nException: " + str(e))
            print("Giving up");
            exit(1)

    def load_list(self, mfn):
        self.mlist = []
        try: 
            for line in open(mfn, "rt", encoding="utf-8"):
                mn = line.strip()
                self.mlist.append(mn)
        except Exception as e:
            traceback.print_exc()
            print("Cannot read file <" + mfn  + ">\nException: " + str(e))
            print("Giving up");
            exit(1)

    def rand_dict(self):
        n = len(self.mdict)
        r = random.randrange(0,n)
        x = self.mdict.keys()[r]
        # x = random.choice(list(self.mdict.keys()))
        return x

    def rand_set(self):
        x = random.choice(list(self.mset))
        return x

    def rand_list(self):
        n = random.randrange(0, len(self.mlist))
        return self.mlist[n]

    def rand_del_dict(self):
        x = random.choice(list(self.mdict.keys()))
        self.mdict.pop(x)
        return x

    def rand_del_set(self):
        x = random.choice(list(self.mset))
        self.mset.remove(x)
        return x

    def rand_del_list(self):
        n = random.randrange(0, len(self.mlist))
        x = self.mlist[n]
        self.mlist.pop(n)
        return x

    def rand_del_list_dict(self):
        n = random.randrange(0, len(self.mlist))
        x = self.mlist[n]
        self.mlist.pop(n)
        return x

