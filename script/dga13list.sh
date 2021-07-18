instance=$1
date=$2
eastwest=$3

mkdir -p ~/$instance

ls -d /data/ITHI/results-name/$eastwest/results-$instance/$date* >~/$instance/list-$date

rm -f ~/$instance/tmp-*

python3 dga13_extract_names.py ~/$instance/dga13_sfx-$date.csv  ~/$instance/suffixes-$date.csv ~/$instance/tmp- ../data/dga13_subnets.txt `cat ~/$instance/list-$date`

