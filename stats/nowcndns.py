# Parse the "dns_millions" result and investigate use of the nowcndns server
#

import sys
import json
import traceback
import os
import pandas as pd

def usage():
    print("Usage: nowcndns.py <dns_millions_result> <result_file.csv>\n")

def from_json(js):
    ret = True
    million_range = 6
    is_cn = False
    is_cc = False
    is_now_cn_dns = False

    try:
        jd = json.loads(js)
        if not 'domain' in jd:
            ret = False
        else:
            domain = jd['domain']
            domain = domain.lower()
            if domain.endswith("."):
                domain = domain[:-1]
            is_cn = domain.endswith(".cn")
            is_cc = domain[-3] == '.'
            if 'ns' in jd:
                ns = jd['ns']
                for nsx in ns:
                    nsx = nsx.lower()
                    if nsx.endswith("."):
                        nsx = nsx[:-1]
                    if nsx.endswith("nowcndns.com"):
                        is_now_cn_dns = True
                        break
            if 'range' in jd:
                million_range = jd['range']
    except Exception as e:
        traceback.print_exc()
        print("Cannot parse <" + js + ">")
        print("error: " + str(e));
        ret = False
    return ret, million_range, is_cn, is_cc, is_now_cn_dns

class range_data:
    def __init__(self):
        self.nb = 0
        self.nb_now = 0
        self.nb_cn = 0
        self.nb_cn_now = 0
        self.nb_cc = 0
        self.nb_cc_now = 0
        self.nb_other = 0
        self.nb_other_now = 0

    def add(self, is_cn, is_cc, is_now_cn_dns):
        self.nb += 1
        if is_now_cn_dns:
            self.nb_now += 1
        if is_cn:
            self.nb_cn += 1
            if is_now_cn_dns:
                self.nb_cn_now += 1
        elif is_cc:
            self.nb_cc += 1
            if is_now_cn_dns:
                self.nb_cc_now += 1
        else:
            self.nb_other += 1
            if is_now_cn_dns:
                self.nb_other_now += 1



#
dns_millions_result = sys.argv[1]
result_file = sys.argv[2]
range_table = []
for i in range(0,5):
    range_table.append(range_data())

for line in open(dns_millions_result, "r"):
    ret, million_range, is_cn, is_cc, is_now_cn_dns = from_json(line)
    if ret and million_range >= 0 and million_range < 5:
        range_table[million_range].add(is_cn, is_cc, is_now_cn_dns)

columns =  ["From", "To", "Nb", "Nb_Now", "Nb_CN", "Nb_CN_Now",
           "Nb_CC", "Nb_CC_Now", "Nb_Others", "Nb_Others_Now" ]
t = []
starts_at = 1
ends_at = 100
for i in range(0,5):
    t.append([ starts_at, ends_at, 
        range_table[i].nb, 
        range_table[i].nb_now, 
        range_table[i].nb_cn, 
        range_table[i].nb_cn_now, 
        range_table[i].nb_cc, 
        range_table[i].nb_cc_now, 
        range_table[i].nb_other, 
        range_table[i].nb_other_now ])
    starts_at = ends_at + 1
    ends_at = ends_at*10
df = pd.DataFrame(t, columns=columns)
df.to_csv(result_file)
