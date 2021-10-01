# Definition of LRU list management.

import traceback

class lru_list_entry:
    def __init__(self, target_class):
        self.lru_next = ""
        self.lru_previous = ""
        self.data = target_class()

class lru_list:
    def __init__(self, target_number, target_class):
        self.lru_first = ""
        self.lru_last = ""
        self.target_number = target_number
        self.target_class = target_class
        self.table = dict()

    def add(self, key):
        ret = True
        try:
            # first manage the lru list
            if key in self.table:
                # bring to top
                if not key == self.lru_first:
                    if self.table[key].lru_previous == "":
                        print("Could not promote <" + key + "> (" + self.table[key].lru_previous + "," + self.table[key].lru_next + ") after " + str(len(self.table)) + " (" + self.lru_first + "," + self.lru_last + ")")
                        ret = False
                    try:
                        if key == self.lru_last:
                            self.lru_last = self.table[key].lru_previous
                            self.table[self.lru_last].lru_next = ""
                        else:
                            self.table[self.table[key].lru_next].lru_previous = self.table[key].lru_previous
                            self.table[self.table[key].lru_previous].lru_next = self.table[key].lru_next
                        self.table[key].lru_previous = ""
                        self.table[key].lru_next = self.lru_first
                        self.table[self.lru_first].lru_previous = key
                        self.lru_first = key
                    except:
                        traceback.print_exc()
                        print("Could not promote <" + key + "> (" + self.table[key].lru_previous + "," + self.table[key].lru_next + ") after " + str(len(self.table)) + " (" + self.lru_first + "," + self.lru_last + ")")
                        ret = False
            else:
                # add an entry to the list
                self.table[key] = lru_list_entry(self.target_class)
                if len(self.table) > self.target_number:
                    #if the list is full, pop the least recently used entry
                    old = self.lru_last
                    self.lru_last = self.table[old].lru_previous
                    self.table[self.lru_last].lru_next = ""
                    self.table.pop(old)
                if self.lru_first == "":
                    self.lru_first = key
                    self.lru_last = key
                else:
                    self.table[key].lru_next = self.lru_first
                    self.table[self.lru_first].lru_previous = key
                    self.lru_first = key
        except:
            traceback.print_exc()
            print("Could not add <" + key + "> (" + self.lru_first + "," + self.lru_last + ") after " + str(len(self.table)))
            ret = False
        return ret


