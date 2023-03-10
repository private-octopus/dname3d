#
# Computation of metric m11, DNS SEC Deployment per SLD
#
# The data is computed at the same time as metric M9. For each
# domain slected in the sample, we extract the DS records and read
# selected algorithm. For each category, we compute the number
# of domains served by a specific algorithm.
# 
# We then compute:
# - M11.1.x: % of domains in X deploying algorithm X.

import sys
import dnslook
import json

class m11_computer:
    def __init__(self):
        self.total = 0
        self.tab = []
        self.loaded = []
        self.metric = []
        for i in range(0,6):
            self.tab.append(dict())
            self.metric.append(dict())
            self.loaded.append(0.0)

    def load(self, dns_json):
        for line in open(dns_json, "rt"):
            dns_look = dnslook.dnslook()
            try:
                dns_look.from_json(line)
                if dns_look.million_range >= 0 and dns_look.million_range < 6:
                    nb_algo = len(dns_look.ds_algo)
                    if nb_algo > 0:
                        for algo in dns_look.ds_algo:
                            if not algo in self.tab[dns_look.million_range]:
                                self.tab[dns_look.million_range][algo] = 0.0
                            self.tab[dns_look.million_range][algo] += 1.0/nb_algo
                    self.loaded[dns_look.million_range] += 1
                    self.total += 1
            except Exception as e:
                traceback.print_exc()
                print("Cannot parse <" + line  + ">\nException: " + str(e))
            if self.total%500 == 0:
                sys.stdout.write(".")
                sys.stdout.flush()

    def save_m11(self, m11_date, m11_csv):
        with open(m11_csv, "wt") as F:
            for i in range(0,6):
                algo_list = self.tab[i]
                for algo in algo_list:
                    fraction = algo_list[algo] / self.loaded[i]
                    F.write("M11." + str(i+1) + "," + m11_date + ",v2.0," + str(algo) + "," + str(fraction) + "\n")

# main

if len(sys.argv) != 4:
    print("Usage: " + sys.argv[0] + " YYYY-MM-DD dns_json m11_csv")
    exit(1)
m11_date = sys.argv[1]
dns_json = sys.argv[2]
m11_csv = sys.argv[3]

m11 = m11_computer()
m11.load(dns_json)
print("\nLoaded " + str(m11.total) + " dns_json entries.")
m11.save_m11(m11_date, m11_csv)
print("M11 for " + m11_date + " saved in " + m11_csv)







