# !/bin/bash
NBSAMPLES=$1
CURRENT_DIR=`pwd`
cd
HOMEDIR=`pwd`
echo "Homedir: $HOMEDIR"
cd $CURRENT_DIR

TODAY=$(date +%y-%m-%d)
YEAR=$(date -d $TODAY +%Y)
MM=$(date -d $TODAY +%m)
FIRST_DAY=$(date -d $YEAR-$MM-01 +%Y-%m-%d)
DAY_AFTER_MONTH=$(date -d "$FIRST_DAY +1 months" +%Y-%m-01)
DAY=$(date -d "$DAY_AFTER_MONTH -1 day" +%d)
YYYYMM="$YEAR$MM"
echo "$YEAR-$MM-$DAY ($YYYYMM)"
OLD_DIR=`pwd`
cd $HOMEDIR/dname3d/stats
echo "Switched from $OLD_DIR to `pwd`"

IP2AS="$HOMEDIR/ip2as/ip2as_$YEAR$MM.csv"
IP2AS6="$HOMEDIR/ip2as/ip2as6_$YEAR$MM.csv"
if [ -f $IP2AS -a -f $IP2AS6 ];
then
    echo "$IP2AS already downloaded";
else
    echo "Need to download $IP2AS or $IP2AS6"
    /usr/local/python3.8/bin/python3 geti2as.py $IP2AS $IP2AS6 $HOMEDIR/temp/
fi

MILLION="$HOMEDIR/majestic/million_$YEAR$MM.txt"
if [ -f $MILLION ];
then
    echo "$MILLION already downloaded";
else
    echo "Need to download $MILLION"
    /usr/local/python3.8/bin/python3 getmajestic.py $MILLION
fi


COM_SAMPLES="$HOMEDIR/com_samples/com_samples_$YYYYMM.csv"
COM_STATS="$HOMEDIR/com_stats/com_stats_$YYYYMM.csv"
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
        Y="$HOMEDIR/com_temp/latest_com_zone"
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
            S_TEMP=$HOMEDIR/tmp/tmp_com_zone_
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
            Z_TEMP=$HOMEDIR/tmp/tmp_com_sample_
            rm -f $Z_TEMP*
            /usr/local/python3.8/bin/python3 do_zonesampler.py $COM_SAMPLES $X 1000000 $Z_TEMP
            rm -f $Z_TEMP*
        fi
    fi

    # clean up the temporrary files to save disk space
    rm -f $HOMEDIR/com_temp/*
fi

RESULT="$HOMEDIR/dns_millions/dns_millions_$YYYYMM.csv"

if [ -f $COM_SAMPLES -a -f $MILLION ];
then
echo "Adding $NBSAMPLES to the DNS processed list"
    #../script/central_million.sh $YYYYMM $NBSAMPLES
    TEMP=$HOMEDIR/tmp/dnslookup_
    rm $TEMP*
    /usr/local/python3.8/bin/python3 do_dnslookup.py $NBSAMPLES $IP2AS $IP2AS6 $PUB_S $MILLION $COM_SAMPLES $RESULT $TEMP
fi

RESULT_NS="$HOMEDIR/ns_list/ns_list_$YYYYMM.csv"

if [ -f $RESULT ]
then
    TEMP=$HOMEDIR/tmp/find_ns_servers_
    rm $TEMP*
    /usr/local/python3.8/bin/python3 find_ns_servers.py $NBSAMPLES $IP2AS $IP2AS6 $PUB_S $RESULT $RESULT_NS $TEMP
fi

if [ -f $RESULT -a -f $RESULT_NS ]
then
    echo "Recomputing M9 for $YEAR-$MM-$DAY"
    m9_day="$YEAR-$MM-$DAY"
    m9_file="$HOMEDIR/M9/M9-$m9_day.csv"
    ip_list="$HOMEDIR/ip_list_$m9_day"
    echo "Computing M9 in $m9_file from $com_stats and $mill_stats"
    /usr/local/python3.8/bin/python3 compute_m9.py $PUB_S $DUP_S ../data/asnames.txt $RESULT $RESULT_NS $m9_file $m9_day $ip_list
fi

if [ -f $RESULT ]
then
    echo "Recomputing M11 for $YEAR-$MM-$DAY"
    m11_day="$YEAR-$MM-$DAY"
    root_stats="$HOMEDIR/dns_root/root_stats_$YYYYMM.csv"
    m11_file="$HOMEDIR/M11/M11-$m11_day.csv"
    echo "Computing M11 in $m11_file from $RESULT and $root_stats"
    /usr/local/python3.8/bin/python3 ./compute_m11.py $m11_day $RESULT $PUB_S $DUP_S $root_stats $m11_file
fi

if [ -f $RESULT ]
then
    cd $HOMEDIR/
    echo "Writing M9 to ITHI staging server"
    rsync -Cav -e "ssh -l octo0" M9 octo0@ithi.research.icann.org:data
    # scp $HOMEDIR/M9/M9-$YEAR-$MM-$DAY.csv octo0@ithi.research.icann.org:data/M9/
    echo "M9 updated."
    echo "Writing M11 to ITHI staging server"
    rsync -Cav -e "ssh -l octo0" M11 octo0@ithi.research.icann.org:data
    # scp $HOMEDIR/M11/M11-$YEAR-$MM-$DAY.csv octo0@ithi.research.icann.org:data/M11/
    echo "M11 updated."
fi


cd $OLD_DIR
pwd