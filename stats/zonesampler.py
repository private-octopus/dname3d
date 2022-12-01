#!/usr/bin/python
# coding=utf-8
#
# Parse the zone file, retrieve the NS records, then extract a sample of domain names.
#
# The naive solution would be to read the entire set of names in memory, but for the
# com zone there are millions of them. We thus have the following constraints:
#
# 1- allow work by partitions.
# 2- only store a plausible number of names.
# 3- be as close as possible to pure random sampling.
#
# The classic algorithm for finding N samples out of a large population is:
#
# - initialize a table of size N by loading the first N elements from the list.
# - after that, consider each successive item of rank K in the database --
#   the first sample after the first N elements will have rank K+1.
# - Keep that sample with probability N/K.
# - If it is kept, pick one of the selected samples at random and replace it.
#
# That works, but the zone file could be very large. It might have to be split
# in a set of equal size partition. We could just pick an equal number of
# samples from each partitions, but that would not give the same results as
# picking at random from the whole set. Instead, we draw at random the number of
# samples from each partition, using a simple process: if N samples are needed,
# assign each of them uniformly at random to one of the partitions, thus obtaining
# a randomized number of sample per partitions. There might be faster algorithms,
# but this one is merely O(nb_samples), which is good enough for our purposes.
#
# Once the required number of samples have been drawn from each partition, we can
# merge them in a single list, and then shuffle that list.
#
# Of course, all this sampling requires computing small probabilities, such
# as 1000/500,000,000 = 1/500,000. That means we need a good unbiased
# random number generator. Fortunately, Python uses the Mersenne Twister 
# as the core generator. It produces 53-bit precision floats and has a
# period of 2**19937-1.
#
# By definition, the same seed will produce the same sequence. On one hand, in
# test mode, we want this reproducibility for tests, so we want an option to
# enter a specific seed -- probably an integer. On the other hand, we want
# regular operation to use unpredictable seed, such as a 16 bytes binary
# string obtained from os.urandom(16).

import traceback
import random
import os
import sys
import math
import dnslook
import json

class one_zone_sample:
    def __init__(self, name, ns_name):
        self.domain = name
        self.ns = [ ns_name ]

    def add_ns(self, name, ns_name):
        ret = False
        if name == self.domain:
            self.ns.append(ns_name)
            ret = True
        return ret

    def to_json(self):
        # use a subset of the json format of dnslook objects
        js = "{\"domain\":\"" + self.domain + "\""
        js += ",\"ns\":" + dnslook.dnslook.to_json_array(self.ns)
        js += "}"
        return js

    def from_json(self, line):
        # Apply subset of dnslook from json logic.
        ret = False
        try:
            jd = json.loads(line)
            if 'domain' in jd and 'ns' in jd:
                ret = True
                self.domain = jd['domain']
                self.ns = jd['ns']
        except Exception as e:
            traceback.print_exc()
            print("Cannot parse <" + line.strip + ">")
        return ret

class zone_sampler:
    def __init__(self, N, seed=0):
        self.N = N
        self.K = 0
        self.is_full = False
        self.samples = []
        self.previous = one_zone_sample("", "")
        if seed == 0:
            seed = os.urandom(16)
        self.rand = random.Random(seed)

    def propose(self, name, ns_name):
        if self.previous.add_ns(name, ns_name):
            return
        new_name = one_zone_sample(name, ns_name)
        self.previous = new_name
        self.K += 1
        if not self.is_full:
            self.samples.append(new_name)
            self.is_full = len(self.samples) >= self.N
            # print("Sample[" + str(self.K -1) + "]= " + name)
        else:
            r = self.rand.random()
            if r <= self.N/self.K:
                u = self.rand.randrange(0, self.N)
                # old = self.samples[u]
                # print("Sample[" + str(u) + "]= " + name + " instead of " + old)
                self.samples[u] = new_name

    def save(self, file_name):
        with open(file_name, "wt") as file:
            for one_name in self.samples:
                file.write(one_name.to_json() + "\n")

    def load_partial_result(self, result_file):
        for line in open(result_file, "rt"):
            one_name = one_zone_sample("","")
            if one_name.from_json(line):
                self.samples.append(one_name)

    def add_zone_file(self, file_name, p_start=0, p_end=0):
        file = open(file_name , "rt", encoding="utf-8")
        file_pos = 0
        if p_start != 0:
            file.seek(p_start)
            file_pos = p_start
        for line in file:
            file_pos += len(line)
            # parse the input line
            parts = line.split("\t")
            # if this is a "NS" record, submit the name.
            if len(parts) == 5 and parts[2] == "in" and parts[3] == "ns":
                self.propose(parts[0], parts[4].strip())
            if p_end != 0 and file_pos >= p_end:
                break
        file.close()

    def shuffle(self):
        self.rand.shuffle(self.samples)

    # compute the number of samples per bucket
    def samples_per_bucket(self, nb_samples, n_p, seed = 0):  
        ns_p = []
        for i in range(0,n_p):
            ns_p.append(0)

        for i in range(0, nb_samples):
            u = self.rand.randrange(0, n_p)
            ns_p[u] += 1

        return ns_p

# simple test program
def basic_test(nb_samples, n_p, zone_file, sample_file, partition_files):
    zs = zone_sampler(12, 12345678901)
    zs.add_zone_file(zone_file)
    zs.shuffle()
    zs.save(sample_file)

    with open(partition_files, "wt") as file:
        file.write("n_p, nb_samples, l_n_sp, n_sp_x\n")
        n_sp = zs.samples_per_bucket(nb_samples, n_p, 0)
        file.write(str(n_p) + "," + str(nb_samples) + "," + str(len(n_sp)) + "\n")
        for i in range(0, len(n_sp)):
            file.write(str(n_p) + "," + str(nb_samples) + "," + str(i) + "," + str(n_sp[i]) + "\n")

# basic test
# basic_test(10000, 50, sys.argv[1], sys.argv[2], sys.argv[3])