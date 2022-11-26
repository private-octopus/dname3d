#!/usr/bin/python
# coding=utf-8
#
# Implementation of the hyperloglog algorithm

import math

class hyperloglog:
    def __init__(self, k):
        self.k = k
        self.m = 1<<k
        self.mk = self.m - 1
        self.b = []
        for i in range(0,self.m):
            self.b.append(0)
        self.alpha = 1.0
        if k == 4:
            self.alpha = 0.673
        elif k == 5:
            self.alpha = 0.697
        elif k == 6:
            self.alpha = 0.709
        elif k >= 7:
            self.alpha = 0.7213/(1 + 1.079/self.m)

    def rho(h):
        #return rank of leftmost bit set to 1
        i = 1
        while h&1 == 0 and i < 30:
            i += 1
            h >>= 1
        return i

    def fnv1a64(x):
        h = 14695981039346656037
        y = str(x)
        for letter in y:
            h ^= ord(letter)
            h *= 1099511628211
            h &= 0xffffffffffffffff
        return h

    def add(self,x):
        h = hyperloglog.fnv1a64(x)
        ib = h&self.mk
        hb = h>>self.k
        zb = hyperloglog.rho(hb)
        if zb > self.b[ib]:
            self.b[ib] = zb

    def evaluate(self):
        a = 0.0
        for z in self.b:
            a += 1.0/(1<<z)
        e = self.alpha*self.m*self.m/a
        if e < (5*self.m/2):
            v = 0
            for z in self.b:
                if z == 0:
                    v += 1
            if v > 0:
                e = self.m*math.log(self.m/v) 
        e = int(e + 0.5)
        return e

    def nb_buckets(self):
        return self.m
    
    def merge_vector(self, v):
        for i in range(0,self.m):
            if v[i] > self.b[i]:
                self.b[i] = v[i]

    def merge(self, other):
        self.merge_vector(other.b)

    def to_text(self):
        s = ""
        for i in range(0,self.m):
            if self.b[i]:
                if s != "":
                    s += ","
                s += str(i) + "," + str(self.b[i])
        return s

    def from_parts(self, parts):
        np = len(parts)
        p = 0
        while p + 2 <= np and len(parts[p]) > 0:
            x = int(parts[p])
            self.b[x] = int(parts[p+1])
            p += 2

    def to_full_text(self):
        s = ""
        for i in range(0,self.m):
                if s != "":
                    s += ","
                s += str(self.b[i])
        return s

    def from_full_parts(self, parts):
        for p in range(0,self.m):
            if p <= len(parts):
                self.b[p] = int(parts[p])
            else:
                self.b[p] = 0

    def header_full_text(self, prefix):
        s = ""
        for i in range(0, self.m):
            if s != "":
                s += ","
            s += prefix + str(i)
        return s

