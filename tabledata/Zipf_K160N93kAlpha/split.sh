echo "Splitting Zipf data into 1000files for parallel processing"
split -a 3 -n l/1000 K160N93kAlpha1.1.tbl K160N93kAlpha1.1.part -d --additional-suffix=.tbl
split -a 3 -n l/1000 K160N93kAlpha1.2.tbl K160N93kAlpha1.2.part -d --additional-suffix=.tbl
split -a 3 -n l/1000 K160N93kAlpha1.3.tbl K160N93kAlpha1.3.part -d --additional-suffix=.tbl
split -a 3 -n l/1000 K160N93kAlpha1.4.tbl K160N93kAlpha1.4.part -d --additional-suffix=.tbl
split -a 3 -n l/1000 K160N93kAlpha1.5.tbl K160N93kAlpha1.5.part -d --additional-suffix=.tbl
split -a 3 -n l/1000 K160N93kAlpha1.6.tbl K160N93kAlpha1.6.part -d --additional-suffix=.tbl
split -a 3 -n l/1000 K160N93kAlpha1.7.tbl K160N93kAlpha1.7.part -d --additional-suffix=.tbl
split -a 3 -n l/1000 K160N93kAlpha1.8.tbl K160N93kAlpha1.8.part -d --additional-suffix=.tbl
split -a 3 -n l/1000 K160N93kAlpha1.9.tbl K160N93kAlpha1.9.part -d --additional-suffix=.tbl
split -a 3 -n l/1000 K160N93kAlpha2.0.tbl K160N93kAlpha2.0.part -d --additional-suffix=.tbl
split -a 3 -n l/1000 K160N93kAlpha2.2.tbl K160N93kAlpha2.2.part -d --additional-suffix=.tbl
split -a 3 -n l/1000 K160N93kAlpha2.4.tbl K160N93kAlpha2.4.part -d --additional-suffix=.tbl
split -a 3 -n l/1000 K160N93kAlpha2.6.tbl K160N93kAlpha2.6.part -d --additional-suffix=.tbl
split -a 3 -n l/1000 K160N93kAlpha2.8.tbl K160N93kAlpha2.8.part -d --additional-suffix=.tbl
split -a 3 -n l/1000 K160N93kAlpha3.0.tbl K160N93kAlpha3.0.part -d --additional-suffix=.tbl
rm K160N93kAlpha1.1.tbl
rm K160N93kAlpha1.2.tbl
rm K160N93kAlpha1.3.tbl
rm K160N93kAlpha1.4.tbl
rm K160N93kAlpha1.5.tbl
rm K160N93kAlpha1.6.tbl
rm K160N93kAlpha1.7.tbl
rm K160N93kAlpha1.8.tbl
rm K160N93kAlpha1.9.tbl
rm K160N93kAlpha2.0.tbl
rm K160N93kAlpha2.2.tbl
rm K160N93kAlpha2.4.tbl
rm K160N93kAlpha2.6.tbl
rm K160N93kAlpha2.8.tbl
rm K160N93kAlpha3.0.tbl


