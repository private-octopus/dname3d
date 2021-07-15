#!/usr/bin/python
# coding=utf-8
#
# Try hash, check that it makes sense.

import sys
import math
import random
import hyperloglog

def one_trial(target):
    dnsletter = "ABCDEFGHIJKLMNOPQRSTUVW0123456789-"
    hll = hyperloglog.hyperloglog(4)
    n = ""
    for i in range(0,target):
        if len(n) >= 16:
            n = n[8:]
        n += random.choice(dnsletter)
        # estimate using hyperloglog class
        hll.add(n)
    return hll.evaluate()


# main

target = int(sys.argv[1])
for attempts in range(0,10):
    print("Target: " + str(target) + ", eval: " + str(one_trial(target)))