# /bin/bash!
#
# Monthly script.
# Run after using "central_dns.sh" and "central_million.sh" to
# collect the DNS data about com zone and  majectic millions.
year=$1
month=$2
day=$3
m9_day="$1-$2-$3"
dns_month="$1$2"

CURRENT_DIR=`pwd`
cd
HOMEDIR=`pwd`
echo "Homedir: $HOMEDIR"
cd $CURRENT_DIR

com_stats="$HOMEDIR/com_stats/com_stats_$dns_month.csv"
mill_stats="$HOMEDIR/dns_millions/dns_millions_$dns_month.csv"
m9_file="$HOMEDIR/M9/M9-$m9_day.csv"
echo "Computing M9 in $m9_file from $com_stats and $mill_stats"
pub_s=../data/public_suffix_list.dat
mill=../data/million.txt
dups=../data/service-duplicates.csv
python ./dnslookup_stats.py $mill_stats $mill $pub_s $dups $com_stats $m9_file $m9_day

