echo "Splitting Zipf data into 1000files for parallel processing"
split -a 3 -n l/1000 K320N93kAlpha1.1.tbl K320N93kAlpha1.1.part -d --additional-suffix=.tbl

