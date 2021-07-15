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

    def merge(self, other):
        for i in range(0,self.m):
            if other.b[i] > self.b[i]:
                self.b[i] = other.b[i]

    def to_text(self):
        s = ""
        for i in range(0,self.m):
            if self.b[i]:
                if s != "":
                    s += ","
                s += str(i) + "," + str(self.b[i])
        return s

    def from_parts(self, parts):
        for p in range(0,len(parts),2):
            x = int(parts[i])
            self.b[x] = int(parts[i+1])




