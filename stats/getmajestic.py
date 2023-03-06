#!/usr/bin/python
# coding=utf-8
#
# Get the "majestic million" file in CSV format from
# http://downloads.majestic.com/majestic_million.csv
# Parse the CSV file, retain the "domain" column
# Save the names in the specified million_file

import sys
import pandas
import traceback

if len(sys.argv) != 2:
    print("Usage: " + sys.argv[0] + " million_file")
    exit(1)

million_file = sys.argv[1]
url = "https://downloads.majestic.com/majestic_million.csv"
column_name = "Domain"
names = []

try:
    c=pandas.read_csv(url)
except Exception as e:
    traceback.print_exc()
    print("Cannot parse: <" + url + "> as CSV\nException: " + str(e))
    exit(1);

print(c)

column_index = 0
for key in c.columns:
    if key == column_name :
        break
    column_index += 1

if len(c.columns) <= column_index :
    print("Cannot find the " + column_index  + " column. Keys are: ")
    for key in c.columns:
        print("    " + key)
    exit(1)

names = c[column_name].values[:, None]
print(names)

try:
    f_out = open(million_file, "wt")
except Exception as e:
    traceback.print_exc()
    print("Cannot open: <" + million_file + "> for writing\nException: " + str(e))
    exit(1);

nb_names = 0
try:
    for name in names:
        if (nb_names < 10):
            print(name[0])
        f_out.write(str(name[0]) + "\n")
        nb_names += 1
except Exception as e:
    traceback.print_exc()
    print("Error writing to: <" + million_file + ">\nException: " + str(e))
f_out.close()
print("Names written: " + str(nb_names))


