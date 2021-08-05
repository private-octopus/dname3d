dates=$1
eastwest=$2
for d in `cat $dates`;
do
../script/daystats_all.sh $d $eastwest
done

