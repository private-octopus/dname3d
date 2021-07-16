#!/usr/bin/python
# coding=utf-8
#
# Generate a test file of names including DGA13

import random

suffixes = [
    "COM", "NET", "CN", "QQ.COM", "ORG", "ROOT-SERVERS.NET",
    "AMAZONAWS.COM", "COM.CN", "XDWSCACHE.OURGLB0.COM", "RU", "EDU",
    "S3-WEBSITE-US-EAST-1.AMAZONAWS.COM", "MICROSOFT.COM",  "IP4.NYUCD.NET",
    "GTLD-SERVERS.NET", "GOOGLE.COM", "DE", "S3.AMAZONAWS.COM", "IN-ADDR.ARPA"]

legit_name = [ 
    "", "WWW.", "NS1.", "AAA.", "ONE-LONG-NAME.", "X.", "Y.", "ZZZ."]

dga_ip = [
    "2607:f8b0:4002::1", "172.217.36.1", "74.125.47.1", "2607:f8b0:4001::1", "2607:f8b0:4004::1",
    "2a00:1450:4013::1", "2607:f8b0:400c::1", "74.125.73.1", "2607:f8b0:4003::1", "172.217.41.1",
   "74.125.181.1", "74.125.177.1", "172.217.34.1", "172.253.248.1", "74.125.42.1", "172.253.215.1"]

legit_ip = [
    "222.212.24.23", "108.161.244.53", "220.188.114.18", "199.59.150.203", "216.218.40.74",
    "205.161.14.145", "184.100.212.196", "121.26.225.2", "202.96.113.99", "114.125.195.199",
    "193.254.189.141", "185.25.32.40", "217.12.97.15", "121.26.225.2", "95.143.172.5",
    "220.189.123.21", "206.125.40.131", "218.248.255.211", "125.174.104.107"]

def get_dga13():
    ref = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-"
    s = random.choice(ref[0:-1])
    for i in range(1,11):
        s += random.choice(ref)
    if random.randint(0,31) != 0:
        s += random.choice(ref)
    s += random.choice(ref[0:-1])
    return s

for i in range(0,1000):
    print(random.choice(legit_name) + random.choice(suffixes) + ",0,tld," + str(random.randint(1,101)) + "," + random.choice(legit_ip))
    print(get_dga13() + "." + random.choice(suffixes[0:-1]) + ",0,tld,1," + random.choice(dga_ip))

