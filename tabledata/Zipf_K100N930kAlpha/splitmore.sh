echo "Splitting Zipf data into 1000files for parallel processing"
split -a 3 -n l/1000 K100N930kAlpha0.0625.tbl K100N930kAlpha0.0625.part -d --additional-suffix=.tbl
split -a 3 -n l/1000 K100N930kAlpha0.125.tbl K100N930kAlpha0.125.part -d --additional-suffix=.tbl
split -a 3 -n l/1000 K100N930kAlpha0.25.tbl K100N930kAlpha0.25.part -d --additional-suffix=.tbl
rm K100N930kAlpha0.0625.tbl
rm K100N930kAlpha0.125.tbl
rm K100N930kAlpha0.25.tbl