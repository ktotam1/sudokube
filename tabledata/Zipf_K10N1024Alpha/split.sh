echo "Splitting Zipf data into 10 files for parallel processing"
split -a 3 -n l/10 K10N1024Alpha0.0.tbl K10N1024Alpha0.0.part -d --additional-suffix=.tbl
rm K10N1024Alpha0.0.tbl



