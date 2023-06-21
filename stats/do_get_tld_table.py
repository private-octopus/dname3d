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
import get_tld_table

# main
monthly_file = sys.argv[1]

tlds = get_tld_table.tld_table()

tlds.load(monthly_file)

print("Found " + str(len(tlds.tld_list)) + " ccTLD or gTLD")

print("Regions: ")
regions = tlds.get_regions()
for region in regions:
    print("    " + region, str(regions[region]))

print("Census CC: ")
census_ccs = tlds.get_cc_lists()
for census_cc in census_ccs:
    census_tlds = census_ccs[census_cc]
    if len(census_tlds) > 1:
        s = ""
        for tld in census_tlds:
            if s != "":
                s += ", "
            s += tld
            if len(tld) > 2:
                if tld in tlds.tld_list:
                    ascii_tld = str(tlds.tld_list[tld].ascii_tld)
                    s += " (" + ascii_tld + ")"
                else:
                    s += " (??)"
        print("    " + census_cc + ": " + s)

