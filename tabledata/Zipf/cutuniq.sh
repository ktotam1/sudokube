echo "Unique values for column $2 for $1 table"
tail -n +2 ../$1.tbl | cut -d\| -f$2  | sort | uniq > $1.$2.uniq


