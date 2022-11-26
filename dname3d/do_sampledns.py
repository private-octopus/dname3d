#!/usr/bin/python
# coding=utf-8
#
# Collecting statistics on AS and CA for a set of DNS names.
#
# For each DNS Name:
#

import sys
import socket
import ip2as
import ipaddress
import os
import concurrent.futures
import traceback
import time
import dns.resolver
import ip2as

class sample_dns_list:
    def __init__(self, i2a, target, id=0):
        self.i2a = i2a
        self.names = []
        self.targets = []
        self.ips = []
        self.as_numbers = []
        self.founds = []
        self.null_ip = ipaddress.IPv4Address("0.0.0.0")
        self.target = target
        self.id = id

    def add_name(self, name):
        self.names.append(name)

    def load(self):
        for name in self.names:
            found = 0
            ip = self.null_ip
            as_number = 0
            target = "www." + name
            for i in range(0,2):
                if i == 1:
                    target = name
                try:
                    self.host = socket.getaddrinfo(target, 443)
                    if len(self.host) > 0:
                        found = 1
                        x = self.host[0]
                        y = x[4]
                        ip = y[0]
                        if len(self.i2a.table) > 0:
                            as_number = self.i2a.get_asn(ip)
                        break
                except Exception as e:
                    pass
            self.targets.append(target)
            self.ips.append(ip)
            self.as_numbers.append("AS" + str(as_number))
            self.founds.append(found)

    def save(self):
        with open(self.target,"wt") as file:
            for i in range(0, len(self.names)):
                file.write(self.targets[i] + "," +  str(self.founds[i]) + "," + \
                           str(self.ips[i]) + "," + self.as_numbers[i] + "\n")

    def load_partial(self, partial_file):
        for line in open(partial_file, "rt"):
            parts = line.split(",")
            if len(parts) == 4:
                self.targets.append(parts[0].strip())
                self.founds.append(int(parts[1].strip()))
                self.ips.append(ipaddress.IPv4Address(parts[2].strip()))
                self.as_numbers.append(parts[3].strip())

def load_dns_bucket(bucket):
    bucket.load()
    bucket.save()

# Main
def main():
    input_names = sys.argv[1]
    ip2as_file = sys.argv[2]
    output_file = sys.argv[3]
    temp_prefix = ""
    if len(sys.argv) == 5:
        temp_prefix = sys.argv[4]

    start_time = time.time()
    log_time = []
    success = 0

    i2a = ip2as.ip2as_table()
    if i2a.load(ip2as_file):
        print("Loaded i2a table of length: " + str(len(i2a.table)))
    else:
        print("Could not load <" + ip2as_file + ">")

    bucket_list = []
    dns_list = sample_dns_list(i2a, output_file)
    ready_time = start_time
    load_time = start_time
    if temp_prefix == "":
        for line in open(input_names, "rt"):
            name = line.strip()
            dns_list.add_name(name.strip())
        ready_time = time.time()
        print("Ready after " + str(ready_time - start_time))
        dns_list.load()

    else:
        nb_process = os.cpu_count()
        print("CPU count is: " + str(nb_process))
        i = 0
        bucket_created = False
        for line in open(input_names, "rt"):
            if i >= nb_process:
                i = 0
                bucket_created = True
            elif not bucket_created:
                target = temp_prefix + str(i) + ".csv"
                sampler = sample_dns_list(i2a, target,id=i)
                bucket_list.append(sampler)
            name = line.strip()
            bucket_list[i].add_name(name)
            i += 1
        ready_time = time.time()
        nb_process = len(bucket_list)
        print("Ready after " + str(ready_time - start_time) + " with " + str(nb_process) + " processes.")
        
        with concurrent.futures.ProcessPoolExecutor(max_workers = nb_process) as executor:
            future_to_bucket = {executor.submit(load_dns_bucket, bucket):bucket for bucket in bucket_list }
            for future in concurrent.futures.as_completed(future_to_bucket):
                bucket = future_to_bucket[future]
                try:
                    data = future.result()
                    sys.stdout.write(".")
                    sys.stdout.flush()
                except Exception as exc:
                    traceback.print_exc()
                    print('\nBucket %d generated an exception: %s' % (bucket.id, exc))
                    exit(1)
        bucket_time = time.time()
        print("\nThreads took " + str(bucket_time - ready_time))   
        # aggregate the results
        for bucket in bucket_list:
            dns_list.load_partial(bucket.target)
    loaded_time = time.time()
    print("Loaded after " + str(loaded_time - ready_time))
    # save the results
    dns_list.save()
    saved_time = time.time()
    print("Saved after " + str(saved_time - loaded_time))
    print("Done in " + str(saved_time - start_time))

# actual main program, can be called by threads, etc.
if __name__ == '__main__':
    main()