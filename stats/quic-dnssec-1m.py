#
# Computation of DNSSEC deployment in 100001 to 1m,
# per TLD
#

import sys
import dnslook

def extract_tld(dns_name):
    tld = ""
    name_parts = dns_name.split(".")
    while len(name_parts) > 0 and len(name_parts[-1]) == 0:
        name_parts = name_parts[:-1]
    if len(name_parts) > 0:
        tld = name_parts[-1]
    return tld


def load_dnsjson(dns_json):
    total = 0
    tld_2ld = dict()
    tld_dnssec = dict()
    for line in open(dns_json, "rt"):
        total += 1
        dns_look = dnslook.dnslook()
        try:
            dns_look.from_json(line)
            if dns_look.million_range == 4:
                tld = extract_tld(dns_look.domain)
                if len(tld) > 0:
                    if tld not in tld_2ld:
                        tld_2ld[tld] = 0
                        tld_dnssec[tld] = 0
                    tld_2ld[tld] += 1
                    if len(dns_look.ds_algo) > 0:
                        tld_dnssec[tld] += 1
        except Exception as e:
            traceback.print_exc()
            print("Cannot parse <" + line  + ">\nException: " + str(e))
        if (total%5000) == 0:
            sys.stdout.write(".")
            sys.stdout.flush()
    print("\nFound " + str(total) + " domains.")
    return tld_2ld, tld_dnssec


dns_json = sys.argv[1]
tld_csv = sys.argv[2]
with open(tld_csv,"wt") as F:
    F.write("Tld, NbSld, NbDNSSEC\n")
    tld_2ld, tld_dnssec = load_dnsjson(dns_json)
    for tld in tld_2ld:
        F.write(tld + str(100*tld_dnssec[tld]/tld_2ld[tld]) + "%," + str(tld_2ld[tld]) + "," + str(tld_dnssec[tld]) + "\n")
