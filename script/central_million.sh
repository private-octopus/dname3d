# /bin/bash!
#
# Start collection of result file collecting DNS lookup results
# from the million list

CURRENT_DIR=`pwd`
cd
HOMEDIR=`pwd`
echo "Homedir: $HOMEDIR"
cd $CURRENT_DIR

ip2as=../data/ip2as.csv
pub_s=../data/public_suffix_list.dat
mill=../data/million.txt
result=$HOMEDIR/dns_millions/dns_millions_$1.csv
temp=$HOMEDIR/tmp/dnslookup_
nb=$2
rm $temp*
python do_dnslookup.py $nb $ip2as $pub_s $mill $result $temp

