
merged=$1
date=$2
homedir=$3
nbsuffix=$4

target=suffixes-$date.csv
echo "Looking for $homedir/*/$target "
list=`ls -d $homedir/*/$target`
python3 merge_suffixes.py $merged $nbsuffix $list



