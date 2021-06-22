date=$1
eastwest=$2

for x in `ls /data/ITHI/results-name/$eastwest/`
do
instance=${x/results-}
../script/daystats.sh $instance $date $eastwest
done


