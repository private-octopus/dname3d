# Manage the list of a million names used for the random trials 
# Trials are made by categories, 0 to 99, 100 to 999, etc.

import random
import traceback
import time

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
        self.million_list = []
        self.already_processed = set()
        self.random_range = 1
        self.range_max = [0,100]
        self.last_pick_rank = 0

    def load(self, mfn):
        million_rank = 0
        current_range = 0
        range_start = 0
        range_end = self.log_first
        self.range_max = [range_start, range_end]
        self.million = dict()
        self.million_list = []
        try: 
            for line in open(mfn, "rt", encoding="utf-8"):
                if million_rank >= range_end:
                    current_range += 1
                    range_start = range_end
                    range_end *= self.log_val
                    self.range_max.append(range_end)
                mn = line.strip()
                if not mn in self.already_processed:
                    self.million_list.append(mn)
                million_rank += 1
            self.range_max[-1] = million_rank
        except Exception as e:
            traceback.print_exc()
            print("Cannot read file <" + mfn  + ">\nException: " + str(e))
            print("Giving up");
            exit(1)

    def set_already_processed(self, mn):
        self.already_processed.add(mn)

    def mark_read(self, mn):
        r = -1
        if self.million_list[self.last_pick_rank] == mn:
            r = self.last_pick_rank
        else:
            for x in range(0, len(self.million_list)):
                if self.million_list[x] == mn:
                    r = x
                    break
        if r >= 0:
            self.million_list.pop(r)

    def random_pick(self):
        a = random.randrange(self.range_max[self.random_range - 1], self.range_max[self.random_range])
        b = a*len(self.million_list)/self.range_max[-1]
        c = int(b)
        if c < 0 or c >= len(self.million_list):
            print("Picking range[" + str(self.random_range) + "] = [ " + str(self.range_max[self.random_range - 1]) + " , " + str(self.range_max[self.random_range]) + " )")
            print(str(c) + " = " + str(a) + " * " + str(len(self.million_list)) + " / " + str(self.range_max[-1]))
            if c < 0:
                c = 0
            else:
                c= len(self.million_list) - 1
        x = self.million_list[c]
        self.last_pick_rank = c
        return x

    def next_random_range(self):
        ret = False
        if len(self.million_list) > 0:
            ret = True
            next_range = self.random_range + 1
            if next_range >= len(self.range_max):
                next_range = 1
            self.random_range = next_range
        return ret

    def nb_ranges(self):
        return len(self.range_max) - 1

    def nb_names(self):
        return len(self.million_list)

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

