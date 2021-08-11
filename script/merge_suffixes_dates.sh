
dates=$1
homedir=$2
nbsuffix=$3

dates=$1
for d in `cat $dates`;
do
    merged="$homedir/suffixes-$d.csv"
    if [ -f merged ]; then
        echo "$merged exists"
    else
        echo "$merged not found. Computing suffixes for $d";
        ../script/merge_suffixes.sh $merged $d $homedir $nbsuffix
    fi
done
