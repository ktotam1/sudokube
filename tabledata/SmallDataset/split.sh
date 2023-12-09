echo "Splitting small dataset into 10 files for parallel processing"
split -a 3 -n l/16 small_dataset.tbl small_dataset.part -d --additional-suffix=.tbl



