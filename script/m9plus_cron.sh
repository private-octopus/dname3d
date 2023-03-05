# !/bin/bash
NBSAMPLES=$1
TODAY=$(date +%d)
YEAR=$(date -d $TODAY +%Y)
MM=$(date -d $TODAY +%m)
FIRST_DAY=$(date -d $YEAR-$MM-01 +%Y-%m-%d)
DAY_AFTER_MONTH=$(date -d "$FIRST_DAY +1 months" +%Y-%m-01)
DAY=$(date -d "$DAY_AFTER_MONTH -1 day" +%d)
YYYYMM="$YEAR$MM"
echo "$YEAR-$MM-$DAY ($YYYYMM)"
OLD_DIR=`pwd`
cd /home/huitema/dname3d/stats
echo "Switched from $OLD_DIR to `pwd`"

MILLION="/home/huitema/majestic/million_$YEAR$MM.txt"
if [ -f $MILLION ];
then
    echo "$MILLION already downloaded";
else
    echo "Need to download $MILLION"
    python3 getmajestic.py $MILLION
fi

#
# TODO locate com zone required for parsing or sampling
#

COM_SAMPLES="/home/huitema/com_samples/com_samples_$YYYYMM.csv"
COM_STATS="/home/huitema/com_stats/com_stats_$YYYYMM.csv"
PUB_S="../data/public_suffix_list.dat"
DUP_S="../data/service-duplicates.csv"
X=""
for i in `ls /data/ZFA-backups/$YYYYMM*/com/com.zone`; do
    if [[ "$i" > "$X" ]]; then
        X="$i"; 
    fi; 
done
if [ -f $COM_STATS ];
then
    echo "COM_STATS already computed";
else
    if [ ! -z "$X" ]; then
        S_TEMP=/home/huitema/tmp_com_zone_
        rm $S_TEMP*
        python3 do_zoneparser.py $COM_STATS $X $PUB_S $DUP_S $MILLION $S_TEMP
        rm $S_TEMP*
    fi
fi

# TODO: use com samples
if [ -f $COM_SAMPLES ];
then
    echo "$COM_SAMPLES already computed";
else
    if [ ! -z "$X" ]; then
        echo "Found COM zone file: $X"
        Z_TEMP=/home/huitema/tmp/tmp_com_sample_
        rm $Z_TEMP*
        python3 do_zonesampler.py $COM_SAMPLES $X 1000000 $Z_TEMP
    fi
fi

RESULT="/home/huitema/dns_millions/dns_millions_$YYYYMM.csv"
if [ -f $COM_SAMPLES -a -f $MILLION ];
then
echo "Adding $NBSAMPLES to the DNS processed list"
    #../script/central_million.sh $YYYYMM $NBSAMPLES
    # TODO: IP2AS should be dynamic, monthly download.
    ip2as=../data/ip2as.csv
    TEMP=/home/huitema/tmp/dnslookup_
    rm $TEMP*
    /usr/local/python3.8/bin/python3 do_dnslookup.py $NBSAMPLES $ip2as $PUB_S $MILLION $COM_SAMPLES $RESULT $TEMP
fi

if [ -f $RESULT ]
then
    echo "Recomputing M9 for $YEAR-$MM-$DAY"
    ../script/compute_m9.sh $YEAR $MM $DAY
    m9_day="$YEAR-$MM-$DAY"
    m9_file="/home/huitema/M9/M9-$m9_day.csv"
    echo "Computing M9 in $m9_file from $com_stats and $mill_stats"
    /usr/local/python3.8/bin/python3 ./dnslookup_stats.py $RESULT $MILLION $PUB_S $DUP_S $COM_STATS $m9_file $m9_day
    echo "Writing M9 to ITHI staging server"
    cd /home/huitema/
    rsync -Cav -e "ssh -l octo0" M9 octo0@ithi.research.icann.org:data
    # scp /home/huitema/M9/M9-$YEAR-$MM-$DAY.csv octo0@ithi.research.icann.org:data/M9/
    echo "M9 updated."
fi

cd $OLD_DIR
pwd