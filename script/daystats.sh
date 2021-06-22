instance=$1
date=$2
eastwest=$3

mkdir -p ~/$instance

ls -d /data/ITHI/results-name/$eastwest/results-$instance/$date* >~/$instance/list-$date

rm ~/$instance/tmp-*

python3 do_namestats.py ~/$instance/stats-$date.csv ~/$instance/tmp- `cat ~/$instance/list-$date`

