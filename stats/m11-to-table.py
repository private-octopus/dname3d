# Process the m11 data, and write them as a table with one line per tracked domain.

import sys

def parse_m11_metric(met):
    met_parts = met.split('.')
    is_parsed = False
    rank = 0
    is_provider = False
    is_share = False
    if len(met_parts) != 3:
        pass
    elif met_parts[0] != 'M11':
        print(met + ": not M11, " + met_parts[0])
    else:
        met_rank = 0
        met_type = 0
        try:
            met_rank = int(met_parts[1])
            met_type = int(met_parts[2])
            is_parsed = True
        except:
            print("Cannot parse rank: " + met_parts[1] + ", or type: " + met_parts[2])
        if is_parsed:
            if met_rank >= 9: 
                rank = int((met_rank - 9)/2)
                is_provider = (met_rank == (9 + 2*rank))
            else:
                print("rank < 9")
                rank = met_rank
                is_parsed = False
        if is_parsed and rank >= 4:
            print(met + " parsed rank >= 4")
            is_parsed = False
        if is_parsed and (met_type != 1 and met_type != 2):
            print(met + " type: " + str(met_type))
            is_parsed = False
        if is_parsed:
            is_share = met_type == 1
    return is_parsed, is_provider, is_share, rank


class m11_domain_line:
    def __init__(self, name):
        self.name = name
        self.share = []
        self.dnssec = []
        for i in range(0,4):
            self.share.append(0.0)
            self.dnssec.append(0.0)

    def add_share(self, rank, share):
        self.share[rank] = share
    def add_dnssec(self, rank, dnssec):
        self.dnssec[rank] = dnssec

    def write_to_file(self, F):
        F.write(self.name)
        for rank in range(0,4):
            F.write(',' + str(self.share[rank]) + ',' + str(self.dnssec[rank]))
        F.write('\n')

        
def add_to_m11_domain(table, name, is_share, rank, f_value):
    if not name in table:
        table[name] = m11_domain_line(name)
    if is_share:
        table[name].add_share(rank, f_value)
    else:
        table[name].add_dnssec(rank, f_value)

class m11_tables:
    def __init__(self):
        self.tld = dict()
        self.provider = dict()

    def add(self, metric, name, met_value):
        is_parsed, is_provider, is_share, rank = parse_m11_metric(metric)
        if is_parsed:
            try:
                f_value = float(met_value)
            except:
                is_parsed = False
        if is_parsed:
            if is_provider:
                add_to_m11_domain(self.provider, name, is_share, rank, f_value)
            else:
                add_to_m11_domain(self.tld, name, is_share, rank, f_value)
        return is_parsed

def m11_domains_write(f_name, table, table_name):
    with open(f_name, 'wt') as F:
        F.write(table_name)
        for r in [ '10k', '90k', '900k', '1M' ]:
            F.write(', share-' + r)
            F.write(', dnssec-' + r)
        F.write('\n')

        has_others = False

        for name in table:
            if name == 'others':
                has_others = True
            else:
                table[name].write_to_file(F)

        if has_others:
            table['others'].write_to_file(F)


file_in = sys.argv[1]
provider_out = sys.argv[2]
tld_out = sys.argv[3]

tables = m11_tables()

skipped_metric = ""
first_skipped = False
nb_reports = 0
ln = 0
for line in open(file_in, 'rt'):
    ln += 1
    parts = line.split(',')
    if len(parts) != 5:
        print("Cannot parse line " + str(ln) + ": " + line.strip())
    elif not tables.add(parts[0].strip(), parts[3].strip(), parts[4].strip()):
        if first_skipped and nb_reports < 10 and skipped_metric != parts[0]:
            print("Skipped line " + str(ln) + ":" + line.strip())
            is_parsed, is_provider, is_share, rank = parse_m11_metric(parts[0])
            print(str(is_parsed) + ", " + str(is_provider) + ", " + str(is_share) + ", " + str(rank))
            skipped_metric = parts[0]
            nb_reports += 1
    elif not first_skipped:
        first_skipped = True
        nb_reports = 0
m11_domains_write(provider_out, tables.provider, 'provider')
m11_domains_write(tld_out, tables.tld, 'tld')





                        