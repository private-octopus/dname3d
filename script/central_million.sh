# /bin/bash!
#
# Start collection of result file collecting DNS lookup results
# from the million list

ip2as=../data/ip2as.csv
pub_s=../data/public_suffix_list.dat
mill=../data/million.txt
result=/home/huitema/dns_millions/dns_millions_$1.csv
temp=/home/huitema/tmp/dnslookup_
nb=$2
rm $temp*
python3 do_dnslookup.py $nb $ip2as $pub_s $mill $result $temp

