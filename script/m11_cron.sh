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
cd
HOMEDIR=`pwd`
echo "Homedir: $HOMEDIR"
cd $HOMEDIR/dname3d/stats
echo "Switched from $OLD_DIR to `pwd`"

COM_SAMPLE="$HOMEDIR/com_samples/com_samples_$YEAR$MM.txt"
if [ -f COM_SAMPLE ];
then
    echo "COM_SAMPLE already created";
else
    echo "Computing COM_SAMPLE"
    rm $HOMEDIR/tmp/czsp_*
    python do_zonesampler.py $COM_SAMPLE /data/ZFA-backups/20220916/com/com.zone 100000 $HOMEDIR/tmp/czsp_
fi
COM_DNS_SAMPLE="$HOMEDIR/com_samples/com_dns_samples_$YEAR$MM.txt"
if [ -f COM_SAMPLE ];
then
    echo "COM_DNS_SAMPLE already created";
else
    echo "Computing COM_DNS_SAMPLE"
    python do_zonesampler.py 
    ../script/central_dns.sh $YYYYMM
fi
