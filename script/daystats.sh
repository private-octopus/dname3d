instance=$1
date=$2
eastwest=$3

mkdir -p ~/$instance

ls -d /data/ITHI/results-name/$eastwest/results-$instance/$date* >~/$instance/list-$date

rm -f ~/$instance/tmp-*

if [ -s ~/$instance/list-$date ]; then
    python do_namestats.py ~/$instance/stats-$date.csv  ~/$instance/suffixes-$date.csv ~/$instance/tmp- ../data/dga13_subnets.txt `cat ~/$instance/list-$date`
else
    echo "empty file: ~/$instance/list-$date"
fi

