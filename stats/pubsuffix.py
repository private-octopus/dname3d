#!/usr/bin/python
# coding=utf-8
#
# The module provides functions to manage the public suffix list

def is_ascii(s):
    return all(ord(c) < 128 for c in s)

def pub_suffix_count(pubsuffixfile):
    count = 0
    for line in open(pubsuffixfile , "rt", encoding='utf-8'):
        if len(line) < 2 or line.startswith("//") or not is_ascii(line):
            continue;
        else:
            count += 1
    return count
