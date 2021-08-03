#!/usr/bin/python
# coding=utf-8
#
# Extract a list of plausible clusters from a list of ip addresses and traffic count


import sys
import ipaddress
import traceback
import time
import concurrent.futures
import os

class ip_cluster:
    def __init__(self, ip, count):
        self.count_min = count
        self.count_max = count
        self.count_margin = 0
        self.IP_first = ip
        self.IP_last = ip

    def belongs(self, ip, count):
        ret = False
        if ip.version == self.IP_first.version:
            try:
                ip_next = self.IP_last + 1
                if ip_next == ip:
                    if count >= self.count_min and count <= self.count_max:
                        ret = true
                    else:
                        # Still OK if +- 1/16
                        # Todo: consider +- sqrt (average)
                        m = int((self.count_min + self.count_max)/2)
                        margin = int(m/16)
                        if count < m:
                            if (count + margin) > m:
                                ret = True
                                self.count_min = count
                        else:
                            if (count - margin) < m:
                                ret = True
                                self.count_max = count
            except:
                pass

        return ret



class ip_list:
    def __init__(self):
        self.IP = ipaddress.ip_address("::")
        self.count = 0
