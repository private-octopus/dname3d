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

COM_STAT="/home/huitema/com_stats/com_stats_$YEAR$MM.csv"
if [ -f $COM_STAT ];
then
    echo "$COM_STAT already created";
else
    echo "Need to compute $COM_STAT"
    ../script/central_dns.sh $YYYYMM
fi
echo "Adding $NBSAMPLES to the million list"
../script/central_million.sh $YYYYMM $NBSAMPLES
echo "Recomputing M9 for $YEAR-$MM-$DAY"
../script/compute_m9.sh $YEAR $MM $DAY
echo "Writing M9 to ITHI staging server"
cd /home/huitema/
rsync -Cav -e "ssh -l octo0" M9 octo0@ithi.research.icann.org:data
# scp /home/huitema/M9/M9-$YEAR-$MM-$DAY.csv octo0@ithi.research.icann.org:data/M9/
echo "M9 updated."
cd $OLD_DIR
pwd
