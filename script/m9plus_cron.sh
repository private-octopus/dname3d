# !/bin/bash
NBSAMPLES=$1
TODAY=$(date +%y-%m-%d)
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

IP2AS="/home/huitema/ip2as/ip2as_$YEAR$MM.csv"
IP2AS6="/home/huitema/ip2as/ip2as6_$YEAR$MM.csv"
if [ -f $IP2AS -a -f $IP2AS6 ];
then
    echo "$IP2AS already downloaded";
else
    echo "Need to download $IP2AS or $IP2AS6"
    /usr/local/python3.8/bin/python3 geti2as.py $IP2AS $IP2AS6 /home/huitema/temp/
fi

MILLION="/home/huitema/majestic/million_$YEAR$MM.txt"
if [ -f $MILLION ];
then
    echo "$MILLION already downloaded";
else
    echo "Need to download $MILLION"
    /usr/local/python3.8/bin/python3 getmajestic.py $MILLION
fi


COM_SAMPLES="/home/huitema/com_samples/com_samples_$YYYYMM.csv"
COM_STATS="/home/huitema/com_stats/com_stats_$YYYYMM.csv"
PUB_S="../data/public_suffix_list.dat"
DUP_S="../data/service-duplicates.csv"

X=""

if [[ ( -f $COM_STATS && -f $COM_SAMPLES ) ]]; then
    echo "Com samples and stats already computed";
else
    XGZ=""
    for i in `ls /data/ZFA-backups/$YYYYMM*/com/com.zone[.gz]*`; do
        if [[ "$i" > "$X" ]]; then
            if [[ "$i" != "$XGZ" ]]; then
                X="$i";
                XGZ="$X.gz";
                if [[ "$X" == *.gz ]]; then
                  XGZ="$X";
                fi;
            fi;
        fi;
    done
    if [[ "$X" == *.gz ]]; then
        Y="/home/huitema/com_temp/latest_com_zone"
        gunzip -c $X > $Y
        X=$Y
    fi
    echo "X: $X"
    echo "XGZ: $XGZ"
    if [ -f $COM_STATS ];
    then
        echo "COM_STATS already computed";
    else
        if [ ! -z "$X" ]; then
            S_TEMP=/home/huitema/tmp/tmp_com_zone_
            rm -f $S_TEMP*
            /usr/local/python3.8/bin/python3 do_zoneparser.py $COM_STATS $X $PUB_S $DUP_S $MILLION $S_TEMP
            rm -f $S_TEMP*
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
            rm -f $Z_TEMP*
            /usr/local/python3.8/bin/python3 do_zonesampler.py $COM_SAMPLES $X 1000000 $Z_TEMP
            rm -f $Z_TEMP*
        fi
    fi

    # clean up the temporrary files to save disk space
    rm -f /home/huitema/com_temp/*
fi

RESULT="/home/huitema/dns_millions/dns_millions_$YYYYMM.csv"
if [ -f $COM_SAMPLES -a -f $MILLION ];
then
echo "Adding $NBSAMPLES to the DNS processed list"
    #../script/central_million.sh $YYYYMM $NBSAMPLES
    TEMP=/home/huitema/tmp/dnslookup_
    rm $TEMP*
    /usr/local/python3.8/bin/python3 do_dnslookup.py $NBSAMPLES $IP2AS $PUB_S $MILLION $COM_SAMPLES $RESULT $TEMP
fi

if [ -f $RESULT ]
then
    echo "Recomputing M9 for $YEAR-$MM-$DAY"
    m9_day="$YEAR-$MM-$DAY"
    m9_file="/home/huitema/M9/M9-$m9_day.csv"
    echo "Computing M9 in $m9_file from $com_stats and $mill_stats"
    /usr/local/python3.8/bin/python3 ./dnslookup_stats.py $RESULT $MILLION $PUB_S $DUP_S $COM_STATS $m9_file $m9_day
fi

if [ -f $RESULT ]
then
    echo "Recomputing M11 for $YEAR-$MM-$DAY"
    m11_day="$YEAR-$MM-$DAY"
    root_stats="/home/huitema/dns_root/root_stats_$YYYYMM.csv"
    m11_file="/home/huitema/M11/M11-$m11_day.csv"
    echo "Computing M11 in $m11_file from $RESULT and $root_stats"
    /usr/local/python3.8/bin/python3 ./compute_m11.py $m11_day $RESULT $PUB_S $DUP_S $root_stats $m11_file
fi

if [ -f $RESULT ]
then
    cd /home/huitema/
    echo "Writing M9 to ITHI staging server"
    rsync -Cav -e "ssh -l octo0" M9 octo0@ithi.research.icann.org:data
    # scp /home/huitema/M9/M9-$YEAR-$MM-$DAY.csv octo0@ithi.research.icann.org:data/M9/
    echo "M9 updated."
    echo "Writing M11 to ITHI staging server"
    rsync -Cav -e "ssh -l octo0" M11 octo0@ithi.research.icann.org:data
    # scp /home/huitema/M11/M11-$YEAR-$MM-$DAY.csv octo0@ithi.research.icann.org:data/M11/
    echo "M11 updated."
fi

cd $OLD_DIR
pwd