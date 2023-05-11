# Test of the IP Network function

import sys
import ipaddress

iplist = sys.argv[1:]

for ip in iplist:
    if ":" in ip:
        net_str = ip+"/48"
    else:
        net_str = ip+"/24"
    net = ipaddress.ip_network(net_str,strict=False)
    print( ip + " ==> " + str(net))

