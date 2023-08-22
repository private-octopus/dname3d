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
import pubsuffix
import zoneparser
import tld_table
import os
import urllib.request, urllib.error, urllib.parse
import traceback

def extract_tld(dns_name):
    tld = ""
    name_parts = dns_name.split(".")
    while len(name_parts) > 0 and len(name_parts[-1]) == 0:
        name_parts = name_parts[:-1]
    if len(name_parts) > 0:
        tld = name_parts[-1]
    return tld

class m11_provider_stat:
    def __init__(self, ns_suffix):
        self.ns_suffix = ns_suffix
        self.sort_weight = 0
        self.count_10k = 0
        self.dnssec_10k = 0
        self.count_1M = 0
        self.dnssec_1M = 0
        self.count = []
        self.dnssec = []
        self.ratio = []
        for i in range(0,5):
            self.count.append(0.0)
            self.dnssec.append(0.0)
            self.ratio.append(0.0)

    def load(self, million_range, dns_sec, weight):
        if million_range >= 0 and million_range < 5:
            self.count[million_range] += weight
            if dns_sec > 0:
                self.dnssec[million_range] += weight

    def compute(self, r_count):
        category_weight = [ 0.1, 1, 1, 1, 1]
        self.sort_weight = 0
        for i in range(0,5):
            if self.count[i] <= 0:
                self.ratio[i] = 0
            else:
                self.ratio[i] = self.dnssec[i]/self.count[i]
                self.sort_weight += category_weight[i]*self.count[i]/r_count[i]

    def compute_10k(self):
        self.count_10k = 0.0
        self.dnssec_10k = 0.0
        for i in range(0,3):
            self.dnssec_10k += self.dnssec[i]
            self.count_10k += self.count[i]
    
    def compute_1M(self):
        self.count_1M = 0.0
        self.dnssec_1M = 0.0
        for i in range(0,5):
            self.dnssec_1M += self.dnssec[i]
            self.count_1M += self.count[i]

class m11_providers_stat:
    def __init__(self):
        self.p_list = dict()
        self.r_count = []
        for i in range(0,5):
            self.r_count.append(0)

    def load(self, million_range, dns_sec, ns_suffix, weight):
        if million_range >= 0 and million_range < 5:
            if not ns_suffix in self.p_list:
                self.p_list[ns_suffix] = m11_provider_stat(ns_suffix)
            self.r_count[million_range] += weight
            self.p_list[ns_suffix].load(million_range, dns_sec, weight)
            
    def top_list(self, n):
        for ns_suffix in self.p_list:
            self.p_list[ns_suffix].compute(self.r_count)
        s_list = sorted(self.p_list.values(), key=lambda x: x.sort_weight, reverse=True)
        if len(s_list) > n:
            other = m11_provider_stat("others")
            for s_data in s_list[n:]:
                for i in range(0,5):
                    other.count[i] += s_data.count[i]
                    other.dnssec[i] += s_data.dnssec[i]
            other.compute(self.r_count)
            s_list = s_list[:n]
            s_list.append(other)
        return s_list
        
    def top_list_10k(self, n, min_count):
        for ns_suffix in self.p_list:
            self.p_list[ns_suffix].compute_10k()
        s_list = sorted(self.p_list.values(), key=lambda x: x.count_10k, reverse=True)
        qualified_number = 0
        for s_data in s_list:
            if s_data.count_10k >= min_count:
                qualified_number += 1
        if n > qualified_number:
            n = qualified_number
        if len(s_list) > n:
            other = m11_provider_stat("others")
            for s_data in s_list[n:]:
                other.count_10k += s_data.count_10k
                other.dnssec_10k += s_data.dnssec_10k
            s_list = s_list[:n]
            s_list.append(other)
        return s_list
        
    def top_list_1M(self, n, min_count):
        for ns_suffix in self.p_list:
            self.p_list[ns_suffix].compute_1M()
        s_list = sorted(self.p_list.values(), key=lambda x: x.count_1M, reverse=True)
        qualified_number = 0
        for s_data in s_list:
            if s_data.count_1M >= min_count:
                qualified_number += 1
        if n > qualified_number:
            n = qualified_number
        if len(s_list) > n:
            other = m11_provider_stat("others")
            for s_data in s_list[n:]:
                other.count_1M += s_data.count_1M
                other.dnssec_1M += s_data.dnssec_1M
            s_list = s_list[:n]
            s_list.append(other)
        return s_list

    def top_list_cat_n(self, cat_n, n, min_count):
        s_list = sorted(self.p_list.values(), key=lambda x: x.count[cat_n], reverse=True)
        qualified_number = 0
        for s_data in s_list:
            if s_data.count[cat_n] >= min_count:
                qualified_number += 1
        if n > qualified_number:
            n = qualified_number
        if len(s_list) > n:
            other = m11_provider_stat("others")
            for s_data in s_list[n:]:
                other.count[cat_n] += s_data.count[cat_n]
                other.dnssec[cat_n] += s_data.dnssec[cat_n]
            s_list = s_list[:n]
            s_list.append(other)
        return s_list

    def save(self, n, title, metric_date, result_file):
        l =  self.top_list(n)
        with open(result_file,"wt") as F:
            s = "date," + title
            level_name = ["top 100", "101 to 1000", "1001 to 10000", "10001 to 100000", "100000 to 1M"]
            for i in range(0,5):
                s += "," + level_name[i]
            for i in range(0,5):
                s += ", C " + level_name[i]
            s += "\n"
            F.write(s)
            for v in l:
                s = metric_date + "," + v.ns_suffix
                for i in range(0,5):
                    s += "," + str(100.0*v.ratio[i])+"%"
                for i in range(0,5):
                    s += "," + str(v.count[i])
                s += "\n"
                F.write(s)

    def save_metric_10k(self, n, min_count, metric_id, metric_date, F):
        l =  self.top_list_10k(n, min_count)
        total_count = 0
        total_dnssec = 0
        for v in l:
            total_count += v.count_10k
            total_dnssec += v.dnssec_10k
        print("Found " + str(len(l)) + " entries for " + metric_id + ", count: " + str(total_count) + ", " + str(total_dnssec) + ", " + str(total_dnssec/total_count))
        if total_count <= 0:
            return
        for v in l:
            count_ratio = v.count_10k/total_count
            F.write(metric_id + ".1," + metric_date + ",v2.0," + v.ns_suffix + "," + str(count_ratio) + "\n")
        for v in l:
            dnssec_ratio = 0
            if v.count_10k > 0:
                dnssec_ratio = v.dnssec_10k/v.count_10k
            F.write(metric_id + ".2," + metric_date + ",v2.0," + v.ns_suffix + "," + str(dnssec_ratio) + "\n")

    def save_metric_1M(self, n, min_count, metric_id, metric_date, F):
        l =  self.top_list_1M(n, min_count)
        total_count = 0
        total_dnssec = 0
        for v in l:
            total_count += v.count_1M
            total_dnssec += v.dnssec_1M
        print("Found " + str(len(l)) + " entries for " + metric_id + ", count: " + str(total_count) + ", " + str(total_dnssec) + ", " + str(total_dnssec/total_count))
        if total_count <= 0:
            return
        for v in l:
            count_ratio = v.count_1M/total_count
            F.write(metric_id + ".1," + metric_date + ",v2.0," + v.ns_suffix + "," + str(count_ratio) + "\n")
        for v in l:
            dnssec_ratio = 0
            if v.count_1M > 0:
                dnssec_ratio = v.dnssec_1M/v.count_1M
            F.write(metric_id + ".2," + metric_date + ",v2.0," + v.ns_suffix + "," + str(dnssec_ratio) + "\n")

    def save_metric_cat_n(self, cat_n, n, min_count, metric_id, metric_date, F):
        l =  self.top_list_cat_n(cat_n, n, min_count)
        total_count = 0
        total_dnssec = 0
        for v in l:
            total_count += v.count[cat_n]
            total_dnssec += v.dnssec[cat_n]
        print("Found " + str(len(l)) + " entries for " + metric_id + ", count: " + str(total_count) + ", " + str(total_dnssec) + ", " + str(total_dnssec/total_count))
        if total_count <= 0:
            return
        for v in l:
            count_ratio = v.count[cat_n]/total_count
            F.write(metric_id + ".1," + metric_date + ",v2.0," + v.ns_suffix + "," + str(count_ratio) + "\n")
        for v in l:
            dnssec_ratio = 0
            if v.count[cat_n] > 0:
                dnssec_ratio = v.dnssec[cat_n]/v.count[cat_n]
            F.write(metric_id + ".2," + metric_date + ",v2.0," + v.ns_suffix + "," + str(dnssec_ratio) + "\n")

class m11_computer:
    def __init__(self, zp):
        self.total = 0
        self.tab = []
        self.loaded = []
        self.metric = []
        self.cctld_algos = dict()
        self.gtld_algos = dict()
        self.tld_list = dict()
        self.zp = zp;
        for i in range(0,6):
            self.tab.append(dict())
            self.metric.append(dict())
            self.loaded.append(0.0)
        self.suffix_stats = m11_providers_stat()
        self.tld_stats = m11_providers_stat()

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
                tlds = tld_table.tld_table()
                tlds.load_from_web()
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
                    support = len(all_tlds[tld]) > 0
                    self.tld_list[tld] = support
                    if len(tld) == 2:
                        nb_cctld += 1
                        m11_computer.add_algo_list_to_dict(self.cctld_algos, all_tlds[tld])
                    elif tld in tlds.tld_list and tlds.tld_list[tld].category == 'gTLD':
                        nb_gtld += 1
                        m11_computer.add_algo_list_to_dict(self.gtld_algos, all_tlds[tld])
                # then normalize the weights based on numbers of tlds
                m11_computer.normalize_dict(self.cctld_algos,nb_cctld)
                m11_computer.normalize_dict(self.gtld_algos,nb_gtld)
                # and finally save the results in the intermediate file
                with open(root_file, "wt") as F:
                    for algo in self.gtld_algos:
                        F.write("gTLD," + str(algo) + "," + str(self.gtld_algos[algo]) + "\n")
                    for algo in self.cctld_algos:
                        F.write("ccTLD," + str(algo) + "," + str(self.cctld_algos[algo]) + "\n")
                    for tld in self.tld_list:
                        i_support = 0
                        if(self.tld_list[tld]):
                            i_support = 1
                        F.write("TLD," + tld + "," + str(i_support) + "\n")
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
                            tld_class = parts[0].strip()
                            if tld_class == "gTLD":
                                algo = int(parts[1])
                                share = float(parts[2])
                                self.gtld_algos[algo] = share
                            elif tld_class == "ccTLD":
                                algo = int(parts[1])
                                share = float(parts[2])
                                self.cctld_algos[algo] = share
                            elif tld_class == "TLD":
                                tld = parts[1].strip()
                                share = float(parts[2])
                                self.tld_list[tld] = (share > 0)
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
        print("Found " + str(len(self.gtld_algos)) + " gtld algos, " + \
            str(len(self.cctld_algos)) + " cctld algos, ")

    def load(self, dns_json):
        domainsFound = dict()
        nb_domains_duplicate = 0
        for line in open(dns_json, "rt"):
            dns_look = dnslook.dnslook()
            try:
                dns_look.from_json(line)
                if dns_look.domain in domainsFound:
                    domainsFound[dns_look.domain] += 1
                    nb_domains_duplicate += 1
                    continue
                else:
                    domainsFound[dns_look.domain] = 1
                if dns_look.million_range >= 0 and dns_look.million_range <= 6:
                    nb_algo = len(dns_look.ds_algo)
                    if nb_algo > 0:
                        for algo in dns_look.ds_algo:
                            if not algo in self.tab[dns_look.million_range]:
                                self.tab[dns_look.million_range][algo] = 0.0
                            self.tab[dns_look.million_range][algo] += 1.0/nb_algo
                    self.loaded[dns_look.million_range] += 1
                    self.total += 1
                dns_sec = 0
                if len(dns_look.ds_algo) > 0:
                    dns_sec = 1
                ns_suffixes = set()
                if dns_look.million_range >= 0 and dns_look.million_range < 5 :
                    weight = 1.0
                    for ns_name in dns_look.ns:
                        ns_suffix = zoneparser.extract_server_suffix(ns_name, self.zp.ps, self.zp.dups)
                        if not ns_suffix in ns_suffixes:
                            ns_suffixes.add(ns_suffix)
                    if len(ns_suffixes) == 0:
                        ns_suffix = zoneparser.extract_server_suffix(dns_look.domain, self.zp.ps, self.zp.dups)
                        ns_suffixes.add(ns_suffix)
                    weight /= len(ns_suffixes)
                    for ns_suffix in ns_suffixes:
                        self.suffix_stats.load(dns_look.million_range, dns_sec, ns_suffix, weight)
                    tld = extract_tld(dns_look.domain)
                    self.tld_stats.load(dns_look.million_range, dns_sec, tld, 1.0)
            except Exception as e:
                traceback.print_exc()
                print("Cannot parse <" + line  + ">\nException: " + str(e))
            if (self.total%5000) == 0:
                sys.stdout.write(".")
                sys.stdout.flush()
        print("\nFound " + str(len(domainsFound)) + " domains, " + str(nb_domains_duplicate) + " duplicates.")

    def save_m11(self, m11_date, m11_csv):
        with open(m11_csv, "wt") as F:
            # Metrics 11.1..11.9 correspond to Gtld, Tld, slices of top 1M, and .com
            # For each algorithm, show the deployment share
            for algo in self.gtld_algos:
                F.write("M11.1," + m11_date + ",v2.0," + str(algo) + "," + str(self.gtld_algos[algo]) + "\n")
            for algo in self.cctld_algos:
                F.write("M11.2," + m11_date + ",v2.0," + str(algo) + "," + str(self.cctld_algos[algo]) + "\n")
            for i in range(0,len(self.tab)):
                algo_list = self.tab[i]
                for algo in algo_list:
                    fraction = algo_list[algo] / self.loaded[i]
                    F.write("M11." + str(i+3) + "," + m11_date + ",v2.0," + str(algo) + "," + str(fraction) + "\n")
            # Metric 11.9 is about the DNSSEC penetration and relative share of
            # the top 20 DNS providers in the top 10K domains
            self.suffix_stats.save_metric_10k(20, 10, "M11.9", m11_date, F)
            # Metric 11.10 is about the DNSSEC penetration and relative share of
            # the top TLDs with at least 10 samples in the top 10K domains
            self.tld_stats.save_metric_10k(200, 10, "M11.10", m11_date, F)
            # Metric 11.11 is about the DNSSEC penetration and relative share of
            # the top 20 DNS providers in the top 90-100K domains
            self.suffix_stats.save_metric_cat_n(3, 20, 90, "M11.11", m11_date, F)
            # Metric 11.12 is about the DNSSEC penetration and relative share of
            # the top TLDs with at least 90 samples in the top 90-100K domains
            self.tld_stats.save_metric_cat_n(3, 200, 90, "M11.12", m11_date, F)
            # Metric 11.13 is about the DNSSEC penetration and relative share of
            # the top 20 DNS providers in the top 900K-1M domains
            self.suffix_stats.save_metric_cat_n(4, 20, 900, "M11.13", m11_date, F)
            # Metric 11.14 is about the DNSSEC penetration and relative share of
            # the top TLDs with at least 900 samples in the top 100K-1M domains
            self.tld_stats.save_metric_cat_n(4, 200, 900, "M11.14", m11_date, F)
            # Metric 11.15 is about the DNSSEC penetration and relative share of
            # the top 20 DNS providers in the top 1M domains
            self.suffix_stats.save_metric_1M(20, 1000, "M11.15", m11_date, F)
            # Metric 11.16 is about the DNSSEC penetration and relative share of
            # the top TLDs with at least 10 samples in the top 1M domains
            self.tld_stats.save_metric_1M(200, 1000, "M11.16", m11_date, F)

# main

if len(sys.argv) != 7:
    print("Usage: " + sys.argv[0] + " YYYY-MM-DD dns_json ps_file dups_file root_stats m11_csv")
    exit(1)
m11_date = sys.argv[1]
dns_json = sys.argv[2]
ps_file = sys.argv[3]
dups_file = sys.argv[4]
root_file = sys.argv[5]
m11_csv = sys.argv[6]

ps = pubsuffix.public_suffix()
ps.load_file(ps_file)
print("found " + str(len(ps.table)) + " public suffixes.")
zp = zoneparser.zone_parser2(ps)
zp.load_dups(dups_file)

m11 = m11_computer(zp)
m11.load_root(root_file)
m11.load(dns_json)
print("\nLoaded " + str(m11.total) + " dns_json entries.")
# Check the results per provider and per TLD
print("Found " + str(len(m11.suffix_stats.p_list)) + " DNS provider suffixes.")
print("Found " + str(len(m11.tld_stats.p_list)) + " TLDs.")
# Save the metric
m11.save_m11(m11_date, m11_csv)
print("M11 for " + m11_date + " saved in " + m11_csv)







