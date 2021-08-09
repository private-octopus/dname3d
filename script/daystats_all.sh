date=$1
eastwest=$2

for x in `ls /data/ITHI/results-name/$eastwest/`
do
instance=${x/results-}
target=~/$instance/suffixes-$date.csv
if [ -f $target ]; then
    echo "$target exists"
else
    echo "$target not found. Computing stats for $instance";
    ../script/daystats.sh $instance $date $eastwest;
fi
done


