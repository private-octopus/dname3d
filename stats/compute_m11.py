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
from tkinter import N
import dnslook
import json
import os
import urllib.request, urllib.error, urllib.parse
import traceback

class m11_computer:
    def __init__(self):
        self.total = 0
        self.tab = []
        self.loaded = []
        self.metric = []
        self.cctld_algos = dict()
        self.gtld_algos = dict()
        for i in range(0,6):
            self.tab.append(dict())
            self.metric.append(dict())
            self.loaded.append(0.0)

    def add_algo_list_to_dict(d, l):
        if len(l) > 0:
            w = 1.0/len(l)
            for algo in l:
                if not algo in d:
                    d[algo] = w
                else:
                    d[algo] += w

    def normalize_dict(d, n):
        for algo in d:
            d[algo] = d[algo]/n

    def load_root(self, root_file):
        # If the root file does not exist, create it by loading
        # the web file https://www.internic.net/domain/root.zone
        # and parsing it.
        if not os.path.isfile(root_file):
            try:
                url = 'https://www.internic.net/domain/root.zone'
                response = urllib.request.urlopen(url)
                root_data = response.read().decode('UTF-8')
                root_lines = root_data.split('\n')
                print("Found " + str(len(root_lines)) + " lines in root data.")
                all_tlds = dict()
                nb_ds_found = 0
                for line in root_lines:
                    # parse as DS record.
                    raw_parts = line.split("\t")
                    parts = []
                    for raw_part in raw_parts:
                        if len(raw_part) > 0:
                            parts.append(raw_part)
                    # retain if name is TLD
                    if len(parts) >= 5 and parts[2] == "IN":
                        name_parts = parts[0].split(".")
                        if len(name_parts) == 2 and len(name_parts[0]) > 0:
                            if not name_parts[0] in all_tlds:
                                all_tlds[name_parts[0]] = []
                            if parts[3] == "DS":
                                # if record is DS, add algo to list
                                content_parts = parts[4].split(" ")
                                if len(content_parts) >= 4:
                                    try:
                                        nb_ds_found += 1
                                        algo = int(content_parts[1])
                                        algo_found = False
                                        for algo_present in all_tlds[name_parts[0]]:
                                            if algo_present == algo:
                                                algo_found = True
                                                break
                                        if not algo_found:
                                            all_tlds[name_parts[0]].append(algo)
                                    except Exception as e:
                                        traceback.print_exc()
                                        print("Invalid algorith <" + line.strip()  + ">\nException: " + str(e))
                print("Found " + str(len(all_tlds)) + " TLDs, " + str(nb_ds_found) + " DS.")

                # now compute the table of algorithms for GTLD and for CCTLD
                nb_cctld = 0
                nb_gtld = 0
                for tld in all_tlds:
                    if len(tld) == 2:
                        nb_cctld += 1
                        m11_computer.add_algo_list_to_dict(self.cctld_algos, all_tlds[tld])
                    else:
                        nb_gtld += 1
                        m11_computer.add_algo_list_to_dict(self.gtld_algos, all_tlds[tld])
                    algo_list = all_tlds[tld]
                # then normalize the weights based on numbers of tlds
                m11_computer.normalize_dict(self.cctld_algos,nb_cctld)
                m11_computer.normalize_dict(self.gtld_algos,nb_gtld)
                # and finally save the results in the intermediate file
                with open(root_file, "wt") as F:
                    for algo in self.gtld_algos:
                        F.write("gTLD," + str(algo) + "," + str(self.gtld_algos[algo]) + "\n")
                    for algo in self.cctld_algos:
                        F.write("ccTLD," + str(algo) + "," + str(self.cctld_algos[algo]) + "\n")
            except Exception as e:
                traceback.print_exc()
                print("Cannot parse <" + url  + ">\nException: " + str(e))
                exit(1)
        else:
            for line in open(root_file, "rt"):
                try:
                    line = line.strip()
                    parts = line.split(",")
                    if len(parts) >= 3:
                        try:
                            algo = int(parts[1])
                            share = float(parts[2])
                            tld_class = parts[0].strip()
                            if tld_class == "gTLD":
                                self.gtld_algos[algo] = share
                            elif tld_class == "ccTLD":
                                self.gtld_algos[algo] = share
                            else:
                                print("Unexpected tld data:" + line )
                                exit(1)
                        except Exception as e:
                            traceback.print_exc()
                            print("Cannot parse tld data:" + line  + "\nException: " + str(e))
                            exit(1)
                except Exception as e:
                    traceback.print_exc()
                    print("Cannot open tld data:" + root_file  + "\nException: " + str(e))
                    exit(1)

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
            for algo in self.gtld_algos:
                F.write("M11.1," + m11_date + ",v2.0," + str(algo) + "," + str(self.gtld_algos) + "\n")
            for algo in self.cctld_algos:
                F.write("M11.2," + m11_date + ",v2.0," + str(algo) + "," + str(self.cctld_algos) + "\n")
            for i in range(0,6):
                algo_list = self.tab[i]
                for algo in algo_list:
                    fraction = algo_list[algo] / self.loaded[i]
                    F.write("M11." + str(i+3) + "," + m11_date + ",v2.0," + str(algo) + "," + str(fraction) + "\n")

# main

if len(sys.argv) != 5:
    print("Usage: " + sys.argv[0] + " YYYY-MM-DD dns_json root_stats m11_csv")
    exit(1)
m11_date = sys.argv[1]
dns_json = sys.argv[2]
root_file = sys.argv[3]
m11_csv = sys.argv[4]

m11 = m11_computer()
m11.load_root(root_file)
m11.load(dns_json)
print("\nLoaded " + str(m11.total) + " dns_json entries.")
m11.save_m11(m11_date, m11_csv)
print("M11 for " + m11_date + " saved in " + m11_csv)







