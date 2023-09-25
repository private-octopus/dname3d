#!/usr/bin/python
# coding=utf-8
#
# Automatic parser for the ASN source at https://bgp.potaroo.net/cidr/autnums.html
# That page contains lines of the form "<a href="/cgi-bin/as-report?as=AS0&view=2.0">AS0    </a> -Reserved AS-, ZZ"

import sys
import traceback
import urllib.request, urllib.error, urllib.parse

def load_url(url, file_name):
    ret = False 
    try:
        with open(file_name,"wb") as F:
            response = urllib.request.urlopen(url)
            file_data = response.read()
            data_length = len(file_data)
            print("Loaded " + str(data_length) + " from " + url)
            F.write(file_data)
            ret = True
    except Exception as e:
        print("Cannot load <" + url + "> in " + file_name + ", exception:" + str(e))
    return ret

# main program

if len(sys.argv) != 3:
    print("Usage: " + sys.argv[0] + " <asnames.html> <asnames.csv>")
    exit(1)

asname_html = sys.argv[1]
asname_csv = sys.argv[2]

asname_url = "https://bgp.potaroo.net/cidr/autnums.html"

if not load_url(asname_url, asname_html):
    exit(1)

with open(asname_csv,"wt") as w_out:
    w_out.write("as_number,as_name,as_country,\n")
    for html_line in open(sys.argv[1]):
        parts = html_line.split(">")
        if len(parts) == 3:
            as_parts = parts[1].split("<")
            name_parts = parts[2].split(",")
            if len(as_parts)==2 and len(name_parts) >= 2:
                as_id = as_parts[0].strip()
                as_country = name_parts.pop().strip()
                as_name = ""
                for n in name_parts:
                    as_name += n
                    as_name += " "
                as_name = as_name.strip()
                if as_id[0:2] == "AS":
                    try:
                        as_number = int(as_id[2:])
                        w_out.write(as_id + "," + as_name + "," + as_country + ",\n")
                    except:
                        traceback.print_exc()
                        print("Cannot parse: " + line.strip())
                        print("as_parts:" + str(as_parts))
                        print("as_id:" + str(as_id))




