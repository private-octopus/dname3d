outfile=$1
suffix_file=$2
nb_suffixes=$3
homedir=$5
date=$4

echo "Looking at instances for first $nb_suffixes in $suffix_file"
target="suffixes-$date.csv"
echo "Looking for $homedir/*/$target "
list=`ls -d $homedir/*/$target`
python suffix_per_instance.py $outfile $suffix_file $nb_suffixes $list



