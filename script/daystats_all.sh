date=$1
eastwest=$2

for x in `ls /data/ITHI/results-name/$eastwest/`
do
instance=${x/results-}
echo "Stats for $instance"
../script/daystats.sh $instance $date $eastwest
done


