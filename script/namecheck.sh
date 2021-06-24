instance=$1
date=$2
eastwest=$3

echo "$instance, $date, $eastwest"

mkdir -p ~/$instance

ls -d /data/ITHI/results-name/$eastwest/results-$instance/$date* >~/$instance/list-$date

wc ~/$instance/list-$date

rm ~/$instance/tmp-*

python3 namecheck.py 4 ~/$instance/names-$date.csv ~/$instance/temp- `cat ~/$instance/list-$date`

