#!/usr/bin/python
# coding=utf-8
#
# The module provides functions to manage the public suffix list

def is_ascii(s):
    return all(ord(c) < 128 for c in s)

def pub_suffix_count(pubsuffixfile):
    count = 0
    for line in open(pubsuffixfile , "rt", encoding='utf-8'):
        if len(line) < 2 or line.startswith("//") or not is_ascii(line):
            continue;
        else:
            count += 1
    return count

def parts_to_domain(np):
    z = "" 
    if len(np) > 0:
        z = np[0]
        if len(np) > 1:
            for p in np[1:]:
                z += p
    return z

class public_suffix:
    def __init__(self):
        self.table = dict()

    def load_file(self, file_name):
        ret = True
        try:
            for line in open(file_name, "rt", encoding='utf-8'):
                l = line.strip()
                s_class = 0
                n = l
                if len(l) < 2 or l.startswith("//") or not is_ascii(l):
                    continue;
                elif l.startswith("!"):
                    n = l[1:]
                    s_class = 2
                elif l.startswith("*."):
                    n = l[2:]
                    s_class = 1
                self.table[n] = s_class
        except Exception as e:
            traceback.print_exc()
            print("Cannot load <" + file_name + ">: " + str(e))
            ret = False
        return(ret)

    # Algorithm
    #
    # Match domain against all rules and take note of the matching ones.
    # If no rules match, the prevailing rule is "*".
    # If more than one rule matches, the prevailing rule is the one which is an exception rule.
    # If there is no matching exception rule, the prevailing rule is the one with the most labels.
    # If the prevailing rule is a exception rule, modify it by removing the leftmost label.
    # The public suffix is the set of labels from the domain which match the labels of the prevailing rule, using the matching algorithm above.
    # The registered or registrable domain is the public suffix plus one additional label.
    def suffix(self, name, test=False):
        is_suffix = False
        n = ""
        if is_ascii(name):
            n = name.lower()
        while n.startswith("."):
            n = n[1:]
        while n.endswith("."):
            n = n[:-1]
        nameparts = n.split(".")
        x = "" 
        if test:
            print("Trying: " + n)
        popped = []
        matched = False
        while len(nameparts) > 0 :
            z = parts_to_domain(nameparts)         
            if test:
                print("Testing: " + z)
            if z in self.table:
                matched = True
                s_class = self.table[z]
                if test:
                    print("Match \"" + z + "\":" + str(s_class) + ", popped: " + str(len(popped)))
                if s_class == 0:
                    # Basic rule like "com" requires just one part, e.g."example.com"
                    if len(popped) == 0:
                        x = z
                    else:
                        is_suffix = True
                        if len(z) > 0:
                            x = popped[-1] + '.' + z
                        else:
                            x = popped[-1]
                    break
                elif s_class == 1:
                    # rules like "*.mm" require 3 parts name, e.g. test.example.mm
                    if len(popped) < 2:
                        if len(popped) > 0:
                            x = popped[0] + "." + z
                        else:
                            x = z
                    else:
                        is_suffix = True
                        x = popped[-2] + "." + popped[-1]
                        if len(z) > 0:
                            x += '.' + z
                    break
                elif s_class == 2:
                    # Apply direct match
                    is_suffix = True
                    x = z
                    break
            elif test:
                print("Entry not in table: \"" + z + "\"")
            popped.append(nameparts.pop(0))
        # if not matched, apply rule "*"
        if not matched:
           nameparts = n.split(".")
           ln = len(nameparts)
           if test:
               print("Not matched, parts: " + str(ln))
           if ln < 2:
               if ln == 1:
                    x = nameparts[0]
               else:
                    x = ""
           else:
               is_suffix = True
               x = nameparts[ln-2] + "." + nameparts[ln-1]
        return x, is_suffix