#!/usr/bin/python
# coding=utf-8
#
# The module dnslookup assesses properties of a domain name: find its root zone, find who manages it,
# find whether the name is an alias, resolve the chain of aliases, find who serves the IP address.
# The goal is to observe concentration in both the name services and the distribution services.
#
# For name services, we capture the list of NS servers for the specified name.
# For distribution services, we capture the list of CNames and the IP addresses.
# We also compute the "server name" and the AS Number. The server name is defined as the public suffix
# of the last CNAME, or that of the actual name if there are no CNAME. The AS Number is that of the
# first IPv4 address returned in the list.
# We format the results as a JSON entry.
#
# The procedures are embedded in the class "dnslook", with three main methods:
#
# get_domain_data(self, domain): read from the DNS the data for the specified domain, and 
# store the results in the referenced object.
# 
# to_json(self): serialize the object as a JSON string.
#
# from_json(self, js): load the object value from a JSON string. 

import sys
import dns.resolver
import json
import traceback
import time

def sanitize(object_may_be_string):
    unsafe_str = str(object_may_be_string)
    safe_str = '' 
    for char in unsafe_str: 
        cp = ord(char) 
        if (cp >= ord('a') and cp <= ord('z')) or \
           (cp >= ord('0') and cp <= ord('9')) or \
           (cp >= ord('A') and cp <= ord('Z')) or \
           cp == ord(':') or cp == ord('.') or \
           cp == ord('-') or cp == ord('_') : 
            safe_str += char 
        elif cp == 9: 
            safe_str += '_'
    return safe_str

class dnslook:
    def __init__(self):
        self.domain = ""
        self.ip = []
        self.ipv6 = []
        self.zone = ""
        self.ns = []
        self.cname = []
        self.server = ""
        self.ds_algo = []
        self.ases = []
        self.resolver=dns.resolver.Resolver()
        self.resolver.timeout = 1
        self.resolver.lifetime = 3
        self.million_rank = -1
        self.million_range = -1

    def to_json_array(x):
        jsa = "["
        is_first = True
        for item in x:
            if not is_first:
                jsa += ","
            is_first = False
            jsa += "\"" + sanitize(item) + "\""
        jsa += "]"
        return(jsa)

    def to_json(self):
        js = "{\"domain\":\"" + self.domain + "\""
        if len(self.ip) > 0:
            js += ",\"ip\":" + dnslook.to_json_array(self.ip)
        if len(self.ipv6) > 0:
            # TODO: bug -- "sanitize" will remove the colons!
            js += ",\"ipv6\":" + dnslook.to_json_array(self.ipv6)
        js += ",\"zone\":\"" + self.zone + "\""
        if len(self.ns) > 0:
            js += ",\"ns\":" + dnslook.to_json_array(self.ns)
        js += ",\"ds_algo\":" + dnslook.to_json_array(self.ds_algo)
        if len(self.cname) > 0:
            js += ",\"cname\":" + dnslook.to_json_array(self.cname)
        js += ",\"server\":\"" + self.server + "\""
        if self.million_rank >= 0:
            js += ",\"rank\":" + str(self.million_rank)
        if self.million_range >= 0:
            js += ",\"range\":" + str(self.million_range)
        if len(self.ases) > 0:
            js += ",\"ases\":" + dnslook.to_json_array(self.ases)
        js += "}"
        return(js)
    
    def from_json(self, js):
        ret = True
        try:
            jd = json.loads(js)
            if not 'domain' in jd:
                ret = False
            else:
                self.__init__()
                self.domain = jd['domain']
                if 'ip' in jd:
                    self.ip = jd['ip']
                if 'ipv6' in jd:
                    self.ipv6 = jd['ipv6']
                if 'zone' in jd:
                    self.zone = jd['zone']
                if 'ns' in jd:
                    self.ns = jd['ns']
                if 'cname' in jd:
                    self.cname = jd['cname']
                if 'server' in jd:
                    self.server = jd['server']
                if 'ases' in jd:
                    self.ases = jd['ases']
                if 'ds_algo' in jd:
                    self.ds_algo = jd['ds_algo']
                if 'rank' in jd:
                    self.million_rank = jd['rank']
                if 'range' in jd:
                    self.million_range = jd['range']

        except Exception as e:
            traceback.print_exc()
            print("Cannot parse <" + js + ">")
            print("error: " + str(e));
            ret = False
        return(ret)

    def get_a(self):
        self.ip = []
        try:
            addresses = self.resolver.query(self.domain, 'A')
            for ipval in addresses:
                self.ip.append(ipval.to_text())
        except Exception as e:
            pass

    def get_aaaa(self):
        self.ipv6 = []
        try:
            addresses = self.resolver.query(self.domain, 'AAAA')
            for ipval in addresses:
                self.ipv6.append(ipval.to_text())
        except Exception as e:
            pass

    def get_ns(self):
        self.ns = []
        nameparts = self.domain.split(".")
        while len(nameparts) > 1 :
            self.zone = ""
            for p in nameparts:
                self.zone += p
                self.zone += '.'
            try:
                nameservers = self.resolver.query(self.zone, 'NS')
                for nsval in nameservers:
                    self.ns.append(sanitize(nsval.to_text()))
                break
            except Exception as e:
                nameparts.pop(0)

    def get_ds_algo(self):
        if self.zone != "":
            # we assume that "get_ds_algo" is called after "get_ns", so
            # we use the same zone definition.
            try:
                ds_recv =  self.resolver.query(self.zone, 'DS')
                for ds in ds_recv:
                    ds_parts = str(ds).split(" ")
                    if len(ds_parts) > 2:
                        self.ds_algo.append(ds_parts[1])
                    else:
                        print("Malformed DS for " + self.zone + ": " + str(ds))
            except dns.resolver.NoAnswer:
                pass
            except Exception as e:
                print("Exception when querying DS for " + self.zone + ": " + str(e))

    def get_cname(self):
        self.cname = []
        candidate = self.domain
        loop_count = 0
        while loop_count < 16:
            try:
                aliases = self.resolver.query(candidate, 'CNAME')
                if len(aliases) > 0:
                    candidate = sanitize(aliases[0].to_text())
                    self.cname.append(candidate)
                    loop_count += 1
                else:
                    break
            except Exception as e:
                break

    def get_server(self, ps, test=False):
        if len(ps.table) > 0:
            if len(self.cname) == 0:
                self.server,is_success = ps.suffix(self.domain)
                if test:
                    print("No cname. Domain: \"" + self.domain + "\", server: \"" + self.server + "\"")
            else:
                self.server,is_success = ps.suffix(self.cname[-1])
                if test:
                    print("Cname[-1]: \"" + self.cname[-1] + "\", server: \"" + self.server + "\"")
        elif test:
            print("Empty table.")

    def get_asn(self, i2a, i2a6):
        as_list = set()
        if len(i2a.table) > 0 and len(i2a6.table) > 0:
            for ipv4 in self.ip:
                print(ipv4)
                as_number = i2a.get_as_number(ipv4)
                as_list.add(as_number)
            for ipv6 in self.ipv6:
                as_number = i2a6.get_as_number(ipv6)
                as_list.add(as_number)
            self.ases = []
            for asn in as_list:
                self.ases.append(asn)
        else:
            print("I2A or I2A6 table is empty")

    def get_domain_data(self, domain, ps, i2a, i2a6, stats, rank=-1, rng=-1):
        self.domain = domain
        if rank >= 0:
            self.million_rank = rank
        if rng >= 0:
            self.million_range = rng
        else:
            print("No range for " + self.domain)

        start_time = time.time()
        self.get_a()
        a_time = time.time()
        self.get_aaaa()
        aaaa_time = time.time()
        self.get_ns()
        ns_time = time.time()
        self.get_ds_algo()
        ds_algo_time = time.time()
        self.get_cname()
        cname_time = time.time()
        self.get_server(ps)
        server_time = time.time()
        self.get_asn(i2a, i2a6)
        asn_time = time.time()
        stats[0] += a_time - start_time
        stats[1] += aaaa_time - a_time
        stats[2] += ns_time - aaaa_time
        stats[3] += ds_algo_time - ns_time
        stats[4] += cname_time - ds_algo_time
        stats[5] += server_time - cname_time
        stats[6] += asn_time - server_time

def load_dns_file(dns_json, dot_after=10000):
    stats = []
    loaded = 0
    domainsFound = dict()
    nb_domains_duplicate = 0
    for line in open(dns_json, "rt"):
        dns_look = dnslook()
        try:
            dns_look.from_json(line)
            if dns_look.domain in domainsFound:
                domainsFound[dns_look.domain] += 1
                nb_domains_duplicate += 1
            else:
                domainsFound[dns_look.domain] = 1
                stats.append(dns_look)
            loaded += 1
        except Exception as e:
            traceback.print_exc()
            print("Cannot parse <" + line  + ">\nException: " + str(e))
        if dot_after > 0 and loaded%dot_after == 0:
            sys.stdout.write(".")
            sys.stdout.flush()
    if dot_after > 0 and loaded%dot_after == 0:
        print(".")
    return stats

class name_table:
    def __init__(self):
        self.table = dict()

    def add_name(self, domain, i2a):
        if not domain in self.table:
            try:
                d = dnslook()
                d.domain = domain
                d.get_a()
                d.get_aaaa()
                d.get_asn(i2a)
                self.table[domain] = d
            except Exception as e:
                traceback.print_exc()
                print("Cannot find addresses of <" + domain  + ">\nException: " + str(e))

    def load(self, file_name, add=True):
        added = load_dns_file(file_name)
        if add:
            for d in added:
                if not d.domain in self.table:
                    self.table[d.domain] = d
        else:
            self.table=added

    def save(self,file_name):
        with open(file_name, "wt") as f_out:
            for domain in self.table:
                try:
                    f_out.write(self.table[domain].to_json() + "\n")
                except Exception as e:
                    traceback.print_exc()
                    print("Cannot save addressed for domain <" + target.domain  + ">\nException: " + str(e))
                    break

    # we expect the addition of new names to run in three steps:
    # - first, get a list of the "dnslook" objects that have not been found
    # - split the list and run separate buckets to search the ns names
    # - then load each of the produced lists back in the table
    #
    def schedule_ns(self, dns_list):
        scheduled = set()
        for d in dns_list:
            for ns in d.ns:
                if not ns in self.table and not ns in scheduled:
                    scheduled.add(ns)
        return list(scheduled)