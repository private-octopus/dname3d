# /bin/bash!
#
# Monthly script. Find the latest available copy of the com zone
# for the specified month (argv[1]), then compute the centralization
# statistics for that zone.

month=$1
x=""
for i in `ls /data/ZFA-backups/$month*/com/com.zone`; do
    if [[ "$i" > "$x" ]]; then
        x="$i"; 
    fi; 
done
temp=~/tmp_com_zone_
pub_s=../data/public_suffix_list.dat
mill=../data/million.txt
dups=../data/service-duplicates.csv
stats="/home/huitema/com_stats/com_stats_$month.csv"
python3 do_zoneparser.py $stats $x $pub_s $mill $dups $temp
rm $temp*
