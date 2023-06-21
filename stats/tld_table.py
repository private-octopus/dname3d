# Parsing the reference list of TLD from IANA, hosted at
# https://www.iana.org/domains/root/db
# This is a web page automatically produced by IANA
# from their internal database. It contains a header
# and a table, with columns "domain", "type" and
# "TLD manager". The domain columns contains
# an HTML anchor tab <a>..</a>, the text part
# is the name of the TLD (unicode) and whose link is of
# the form "/domains/root/db/xn--45br5cyl.html", which
# points to the registration part for the domain and 
# includes the punycode of the TLD. The type columns
# contain one of "country code", "generic", "sponsored",
# and "test". The domain manager column could be useful
# for some statistics, e.g., mapping IDN-CCTLD to the
# corresponding CCTLD.
#
# Instead, get the table from the dns observatory data.
# First get the catalog:
# https://observatory.research.icann.org/dns-core-census/v010/table/catalog.csv
# then find the oldest ZONE entry
# "DNS_CORE_CENSUS_ZONES_2023_06_17_090002_TABLE","ZONES","2023-06-17-090002","2023-06-17","http://observatory.research.icann.org./dns-core-census/v010/table/DNS_CORE_CENSUS_ZONES_2023_06_17_090002_TABLE.csv.gz"
# Extract the URL and obtain the CSV file. Then save it as the specified file name.
# 
# If the file is present, load it. That file contains a table of data in CSV format,
# possibly compressed. That table has 82 colums, but for now we are looking at:
#
# - column 4, ALABEL, xn--2scrj9c.
# - column 6, ULABEL, ಭಾರತ. (appears mangled)
# - Column 7, CENSUS_CATEGORY, cctld
# - column 8, CENSUS_JURISDICTION, IN
# - Column 12, ROOT_DB_ATSIGNID, xn--2scrj9c
# - column 15, ROOT_DB_META_CLASS, country-code
# - column 40, IDN_CCTLD_FAST_TRACK_STRING_IN_ENGLISH, Bharat
# - Column 62, UN_M49_ISO_ALPHA3_CODE, IND
# - Column 63, ICANN_GEOGRAPHIC_REGION, Southern Asia
#

import sys
import pandas as pd
import urllib.request, urllib.error, urllib.parse
import gzip
from io import StringIO
import os.path

class tld_table_entry:
    def __init__(self, tld, category, census_cc, ascii_tld, region):
        self.tld = tld
        self.category = category
        self.census_cc = census_cc
        self.ascii_tld = ascii_tld
        self.region = region

    def title():
        s = "TLD,CATEGORY,CENSUS_CC,ASCII_TLD,REGION"
        return s

    def to_string(self):
        s = self.tld
        s += ',' + self.category
        s += ',' + self.census_cc
        s += ',' + self.ascii_tld
        s += ',' + self.region
        return s

class tld_table:
    def __init__(self):
        self.tld_list = dict()

    def load_df(self, df):
        for i in range(0, len(df)):
            category =  df.CENSUS_CATEGORY[i]
            if df.ICANN_GEOGRAPHIC_REGION[i] != "_not applicable_":
                region = df.ICANN_GEOGRAPHIC_REGION[i]
            if category == "ccTLD" or category == "gTLD" or category == "root" or category == "infrastructure":
                tld = df.ALABEL[i]
                while tld.endswith("."):
                    tld = tld[0:-1]
                category =  df.CENSUS_CATEGORY[i]
                census_cc = df.CENSUS_JURISDICTION[i]
                ascii_tld = tld
                if df.IDN_CCTLD_FAST_TRACK_STRING_IN_ENGLISH[i] != "_not applicable_":
                    ascii_tld = df.IDN_CCTLD_FAST_TRACK_STRING_IN_ENGLISH[i]
                region = ""
                if df.ICANN_GEOGRAPHIC_REGION[i] != "_not applicable_":
                    region = df.ICANN_GEOGRAPHIC_REGION[i]
                if tld in self.tld_list:
                    print(str(k) + ": Duplicate tld: " + tld + " (" + str(tld_list[tld]) + ")" )
                    exit(1)
                else:
                    self.tld_list[tld] = tld_table_entry(tld, category, census_cc, ascii_tld, region)

    def get_regions(self):
        regions = dict()
        for tld in self.tld_list:
            tld_entry = self.tld_list[tld]
            if tld_entry.category == "ccTLD" and tld_entry.region != "" :
                if not tld_entry.region in regions:
                    regions[tld_entry.region] = []
                regions[tld_entry.region].append(tld_entry.tld)
        return regions

    def get_cc_lists(self):
        cc_lists = dict()
        for tld in self.tld_list:
            tld_entry = self.tld_list[tld]
            if tld_entry.category == "ccTLD":
                if not tld_entry.census_cc in cc_lists:
                    cc_lists[tld_entry.census_cc] = []
                cc_lists[tld_entry.census_cc].append(tld_entry.tld)
        return cc_lists

    def load_zone_csv_file(self, csv_file):
        df = pd.read_csv(csv_file, keep_default_na=False, na_values=['_'])
        self.load_df(df)

    def load_from_web(self):
        catalog_url = "https://observatory.research.icann.org/dns-core-census/v010/table/catalog.csv"
        catalog_response = urllib.request.urlopen(catalog_url)
        catalog_df = pd.read_csv(catalog_response)
        print("Catalog lines = " + str(len(catalog_df)))
        latest_zone_time = ""
        latest_zone_url = ""
        for i in range(0, len(catalog_df)):
            if catalog_df.TABLE_TOPIC[i] == 'ZONES' and \
                catalog_df.TABLE_TIME[i] > latest_zone_time:
                latest_zone_time = catalog_df.TABLE_TIME[i]
                latest_zone_url = catalog_df.TABLE_URL[i]
        print("For time:" +  latest_zone_time + ", URL: " + latest_zone_url)
        zone_response = urllib.request.urlopen(latest_zone_url)
        zone_gzip = zone_response.read()
        print("Got " + str(len(zone_gzip)) + " compressed bytes")
        zone_bytes = gzip.decompress(zone_gzip)
        print("Got " + str(len(zone_bytes)) + " decompressed bytes")
        zone_text = zone_bytes.decode("utf-8")
        print("Got " + str(len(zone_text)) + " UTF bytes")
        zone_df = pd.read_csv(StringIO(zone_text), keep_default_na=False, na_values=['_'])
        print("Zone lines = " + str(len(zone_df)))
        self.load_df(zone_df)

    def load_file(self, monthly_file):
        df = pd.read_csv(monthly_file, keep_default_na=False, na_values=['_'])
        for i in range(0, len(df)):
            self.tld_list[df.TLD[i]] = tld_table_entry(df.TLD[i], df.CATEGORY[i], df.CENSUS_CC[i], df.ASCII_TLD[i], df.REGION[i])

    def save_file(self, monthly_file):
        with open(monthly_file, 'wt') as F:
            F.write(tld_table_entry.title() + '\n')
            for tld in self.tld_list:
                F.write(self.tld_list[tld].to_string() + '\n')

    def load(self, monthly_file):
        if os.path.isfile(monthly_file):
            self.load_file(monthly_file)
        else:
            self.load_from_web()
            self.save_file(monthly_file)

