echo "Splitting Zipf data into 1000files for parallel processing"
split -a 3 -n l/1000 K320N93kAlpha3.0.tbl K320N93kAlpha3.0.part -d --additional-suffix=.tbl
rm K320N93kAlpha3.0.tbl
