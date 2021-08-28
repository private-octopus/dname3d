# Suffix explainer
#
# The goal is to extract samples of the names and IP addresses associated with the
# top suffixes. We could simply reprocess all files, but that's a lot --
# 220 instances, 31 days, 288 slices = 1964160. Way too many, if the goal is to
# extract maybe a 100 names per suffix, 5000 names, and as many addresses. Thus
# it seems better to proceed by sampling.
#
# The question of course is, how many samples. There are examples of failure
# modes: some suffixes are only found in a few cities. For example, take the
# stats for "decentraland.system" in July 2007:
#
# date      city    hits    subs    ips nets
# all       all     6105617 6452038  74 45
# 20210718  all     6105617 6452038  74 45
# 20210719  all     1620994 1075340  44 19
# 20210720  all     1449327  938478  49 27
# 20210721  all     1463323 2599382  44 22
# 20210722  all     1102649 1009257  22 19
# all       au-mas      163     141   2  2
# all       br-udi       20      33   2  1
# all       ch-mot     4180    4021   1  1
# all       cn-bjd       39      58   1  1
# all       cz-xuy     1109     876   9  9
# all       de-dtm      112     108   2  2
# all       es-mad       28      22   1  1
# all       gb-brf      906     859   3  5
# all       kr-icn    16271   19434  27  9
# all       mx-mty      202     140   2  2
# all       nc-nou      380     265   2  1
# all       rs-beg  6076125 6452038   1  1
# all       us-lax      262     188   1  1
# all       us-rtv     5731    4884   6  6
#
# We will learn a lot by scanning rs-beg on 20210718. But when do we know that
# we had enough? Do we need to identify all addresses, or just the one address
# that acconts for 6M hits? And if we start with a specific day, how many
# files for that day shall we use?
#
# Maybe it is enough to just select a few <city-date> combinations, such as
# for each suffix the first location and the first date. Then, scan all of these
# combinations once, picking one slice for each. Be satisfied when we have enough
# names. If we don't have enough names for some prefixes, repeat, pull more
# slices for the city/name combinations that were selected for these cities.
#
# Then there is the sampling strategy. We want to pick the addresses that issued
# the large number of names, but we do not want to pick too many addresses. Take
# for example .com: billions of hits, 10 of millions of names, millions of IP
# addresses. There is no practical benefit in listing all these million addresses,
# just picking a few of them is enough to show the diversity.
#
# Look then at the top listings for Mail.RU:
#
# Suffix   date   city         hits      subs     ips    nets
# MAIL.RU   all   rs-beg   21454882   6072506    1024     454
# MAIL.RU   all   by-gme    3951634     53767    3329    2258
# MAIL.RU   all   fr-bfc     139172     30352    2798    2306
# MAIL.RU   all   au-kah      49040     22764     695     330
# MAIL.RU   all   fr-mbv     113405     20756     503     371
# MAIL.RU   all   kr-icn     730168     12381    2860     765
# MAIL.RU   all   us-rtv     173821      8947   10612   11091
# MAIL.RU   all   cz-xuy     481616      6231   19740   31272
# MAIL.RU   all   nc-nou      15647      4850     196     130
#
# There are lots of IP addresses involved, but most of them hit relatively few names.
# We want to sample the addresses that are creating the largest numbers of names --
# we suspect that just a small number of addresses hitting rs-beg are responsible
# for most of the names. We want to list those, but how can we do that?
#
# Maybe tie the sampling of addresses to the sampling of names. If a name is being
# added to the sample list, add the corresponding IP address. In our example,
# we have 6 millions names added at rs-beg, out of a total of maybe 6.4 millions.
# We have 21 million hits for the 6M names, versus maybe 6 million hits. If the
# sampling is by hits, more than 70% of samples will show the IP responsible 
# for the new names. Maybe that's enough. Certainly a first step.
#
# Should we do something more complicated? Maybe keep a score per IP address. Add
# the IP address with the name replacement strategy, but keep a running score for
# all addresses in the list. Instead of replacing at random, replace the IP address
# with the lowest score? Maybe use a longuish list and an LRU strategy? Maybe keep
# a new list for each slice and then merge it? Keep a running score of number
# of names for an IP address? Let's do that later.

import random
import nameparse

class suffix_sample:
    def __init__(self, suffix, nb_samples, rd):
        self.suffix = suffix
        self.nb_samples = nb_samples
        self.samples = dict()
        self.pop_size = 0
        self.rd = rd

    def add(self, name, ip):
        if not name in self.samples:
            if len(self.samples) < self.nb_samples:
                self.samples[name] = ip
            else:
                self.pop_size += 1
                x = self.rd.randrange(self.pop_size)
                if x < self.nb_samples:
                    key_out = self.rd.choice(list(self.samples.keys()))
                    self.samples.pop(key_out)
                    self.samples[name] = ip

class suffix_sample_list:
    def __init__(self, nb_samples):
        self.nb_samples = nb_samples
        self.suffixes = dict()
        self.rd = random.Random(123456789)

    def add_suffix(self, suffix):
        if not suffix in self.suffixes:
            self.suffixes[suffix] = suffix_sample(suffix, self.nb_samples, self.rd)

    def add(self, name, ip):
        # find all the embedded suffixes, and if they belong to the list
        # add that to the samples
        name_parts = name.split(".")
        np = len(name_parts)
        i_sfn = 0
        i_start = 0

        while i_start + 1 < np:
            i_sfn += len(name_parts[i_start])+1
            suffix = name[i_sfn:]
            if suffix in self.suffixes:
                self.suffixes[suffix].add(name_parts[i_start], ip)
            i_start += 1

    def add_log_file(self, file_name):
        for line in open(file_name, "rt", encoding="utf-8"):
            nl = nameparse.nameline()
            if nl.from_csv(line) and nl.name_type == "tld":
                self.add(nl.name, nl.ip)

    def save(self, file_name):
        with open(file_name , "wt", encoding="utf-8") as f:
            f.write("suffix" + "," + "name" + "," + "ip" + "\n")
            for suffix in self.suffixes:
                for name in self.suffixes[suffix].samples:
                    f.write(suffix + "," + name + "," + self.suffixes[suffix].samples[name] + "\n")

