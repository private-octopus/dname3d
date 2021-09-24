# Finding name services with similar names, candidates for aggregation
import sys
import functools
import traceback

def find_first_part(name):
    parts = name.split(".")
    x = parts[0]
    return x

class service_line:
    def __init__(self, name, count):
        self.name = name
        self.count = count

class match_line:
    def __init__(self, name, count, max_match, sum_p):
        self.name = name
        self.count = count
        self.max_match = max_match
        self.sum_p = sum_p

def compare_match_line(item,other):
    if item.sum_p < other.sum_p:
        return -1
    elif item.sum_p > other.sum_p:
        return 1
    elif item.max_match < other.max_match:
        return 1
    elif item.max_match > other.max_match:
        return -1
    elif item.count < other.count:
        return -1
    elif item.count > other.count:
        return 1
    elif item.name < other.name:
        return 1
    elif item.name > other.name:
        return -1
    else:
        return 0



# main
out_file = sys.argv[1]
in_file = sys.argv[2]

service_list = []
for line in open(in_file, "rt"):
    parts = line.split(",")
    if parts[0] == "sf":
        service_list.append(service_line(parts[1].strip(), int(parts[2].strip())))
service_list.sort(key=lambda service_line: service_line.name)

last_part = ""
matches = []
max_p = 0
sum_p = 0
max_match = ""
matching = []
for service in service_list:
    x = find_first_part(service.name)
    if x == last_part:
        matches.append(service)
        if service.count > max_p:
            max_match = service.name
            max_p = service.count
        sum_p += service.count
    else:
        if len(matches) > 1 and sum_p > 100000 and 2*sum_p > 3*max_p:
            for match in matches:
                matching.append(match_line(match.name, match.count, max_match, sum_p))
        matches = []
        matches.append(service)
        max_p = service.count
        sum_p = service.count
        max_match = service.name
        last_part = x
if len(matches) > 1 and sum_p > 100000 and 2*sum_p < 3*max_p:
    for match in matches:
        matching.append(match_line(match.name, match.count, max_match, sum_p))

matching.sort(key=functools.cmp_to_key(compare_match_line), reverse=True)
        
f = open(out_file, "wt")
for match in matching:
    f.write(match.name + "," + match.max_match + "," + str(match.count)  + "," + str(match.sum_p) + "\n")

f.close()


