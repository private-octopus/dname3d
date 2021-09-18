#!/usr/bin/python
# coding=utf-8
#
# Obtain the public suffix associated with a domain name.
# This is based on the "Public Suffix List" developed initially by Mozilla and
# maintained at https://publicsuffix.org/list/public_suffix_list.dat.
#
# The class must be initiated by loading a suffix list (see copy in data directory)
# The "suffix" function returns the suffix for a domain name.

import sys
import traceback
import pubsuffix

ps = pubsuffix.public_suffix()

def checkPublicSuffix(d, x):
    if not pubsuffix.is_ascii(d):
        return
    y,is_suffix = ps.suffix(d)
    if x != y:
        print("For <" + d + "> expected <" + x + "> got <" + y + "," + str(is_suffix) + ">")
        ps.suffix(d, test=True)
        exit(1)
    print("For <" + d + "> got <" + y + "," + str(is_suffix) + "> as expected.")

if len(sys.argv) != 2:
    print("Usage: " + sys.argv[0] + " public_suffix_file")
    exit(1)

public_suffix_file = sys.argv[1]

if not ps.load_file(public_suffix_file):
    print("Could not load the suffixes")
    exit(1)

print("Loaded suffixes: " + str(len(ps.table)))

# Any copyright is dedicated to the Public Domain.
# https://creativecommons.org/publicdomain/zero/1.0/

# null input.
checkPublicSuffix("", "");
# Mixed case.
checkPublicSuffix('COM', "");
checkPublicSuffix('example.COM', 'example.com');
checkPublicSuffix('WwW.example.COM', 'example.com');
# Leading dot.
checkPublicSuffix('.com', "");
checkPublicSuffix('.example', "");
checkPublicSuffix('.example.com', "example.com");
checkPublicSuffix('.example.example', "example.example");
# Unlisted TLD.
checkPublicSuffix('example', "");
checkPublicSuffix('example.example', 'example.example');
checkPublicSuffix('b.example.example', 'example.example');
checkPublicSuffix('a.b.example.example', 'example.example');
# Listed, but non-Internet, TLD.
#checkPublicSuffix('local', "");
#checkPublicSuffix('example.local', "");
#checkPublicSuffix('b.example.local', "");
#checkPublicSuffix('a.b.example.local', "");
# TLD with only 1 rule.
checkPublicSuffix('biz', "");
checkPublicSuffix('domain.biz', 'domain.biz');
checkPublicSuffix('b.domain.biz', 'domain.biz');
checkPublicSuffix('a.b.domain.biz', 'domain.biz');
# TLD with some 2-level rules.
checkPublicSuffix('com', "");
checkPublicSuffix('example.com', 'example.com');
checkPublicSuffix('b.example.com', 'example.com');
checkPublicSuffix('a.b.example.com', 'example.com');
checkPublicSuffix('uk.com', "");
checkPublicSuffix('example.uk.com', 'example.uk.com');
checkPublicSuffix('b.example.uk.com', 'example.uk.com');
checkPublicSuffix('a.b.example.uk.com', 'example.uk.com');
checkPublicSuffix('test.ac', 'test.ac');
# TLD with only 1 (wildcard) rule.
checkPublicSuffix('mm', "");
checkPublicSuffix('c.mm', "");
checkPublicSuffix('b.c.mm', 'b.c.mm');
checkPublicSuffix('a.b.c.mm', 'b.c.mm');
# More complex TLD.
checkPublicSuffix('jp', "");
checkPublicSuffix('test.jp', 'test.jp');
checkPublicSuffix('www.test.jp', 'test.jp');
checkPublicSuffix('ac.jp', "");
checkPublicSuffix('test.ac.jp', 'test.ac.jp');
checkPublicSuffix('www.test.ac.jp', 'test.ac.jp');
checkPublicSuffix('kyoto.jp', "");
checkPublicSuffix('test.kyoto.jp', 'test.kyoto.jp');
checkPublicSuffix('ide.kyoto.jp', "");
checkPublicSuffix('b.ide.kyoto.jp', 'b.ide.kyoto.jp');
checkPublicSuffix('a.b.ide.kyoto.jp', 'b.ide.kyoto.jp');
checkPublicSuffix('c.kobe.jp', "");
checkPublicSuffix('b.c.kobe.jp', 'b.c.kobe.jp');
checkPublicSuffix('a.b.c.kobe.jp', 'b.c.kobe.jp');
checkPublicSuffix('city.kobe.jp', 'city.kobe.jp');
checkPublicSuffix('www.city.kobe.jp', 'city.kobe.jp');
# TLD with a wildcard rule and exceptions.
checkPublicSuffix('ck', "");
checkPublicSuffix('test.ck', "");
checkPublicSuffix('b.test.ck', 'b.test.ck');
checkPublicSuffix('a.b.test.ck', 'b.test.ck');
checkPublicSuffix('www.ck', 'www.ck');
checkPublicSuffix('www.www.ck', 'www.ck');
# US K12.
checkPublicSuffix('us', "");
checkPublicSuffix('test.us', 'test.us');
checkPublicSuffix('www.test.us', 'test.us');
checkPublicSuffix('ak.us', "");
checkPublicSuffix('test.ak.us', 'test.ak.us');
checkPublicSuffix('www.test.ak.us', 'test.ak.us');
checkPublicSuffix('k12.ak.us', "");
checkPublicSuffix('test.k12.ak.us', 'test.k12.ak.us');
checkPublicSuffix('www.test.k12.ak.us', 'test.k12.ak.us');

# TODO: add international character support.
# IDN labels.
# checkPublicSuffix('食狮.com.cn', '食狮.com.cn');
# checkPublicSuffix('食狮.公司.cn', '食狮.公司.cn');
# checkPublicSuffix('www.食狮.公司.cn', '食狮.公司.cn');
# checkPublicSuffix('shishi.公司.cn', 'shishi.公司.cn');
# checkPublicSuffix('公司.cn', "");
# checkPublicSuffix('食狮.中国', '食狮.中国');
# checkPublicSuffix('www.食狮.中国', '食狮.中国');
# checkPublicSuffix('shishi.中国', 'shishi.中国');
# checkPublicSuffix('中国', "");
# Same as above, but punycoded.
# checkPublicSuffix('xn--85x722f.com.cn', 'xn--85x722f.com.cn');
# checkPublicSuffix('xn--85x722f.xn--55qx5d.cn', 'xn--85x722f.xn--55qx5d.cn');
# checkPublicSuffix('www.xn--85x722f.xn--55qx5d.cn', 'xn--85x722f.xn--55qx5d.cn');
# checkPublicSuffix('shishi.xn--55qx5d.cn', 'shishi.xn--55qx5d.cn');
# checkPublicSuffix('xn--55qx5d.cn', "");
# checkPublicSuffix('xn--85x722f.xn--fiqs8s', 'xn--85x722f.xn--fiqs8s');
# checkPublicSuffix('www.xn--85x722f.xn--fiqs8s', 'xn--85x722f.xn--fiqs8s');
# checkPublicSuffix('shishi.xn--fiqs8s', 'shishi.xn--fiqs8s');
# checkPublicSuffix('xn--fiqs8s', "");

# Finally done

print("All tests pass.")




