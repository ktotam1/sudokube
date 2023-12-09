#mkdir -p tabledata/Zipf_2/uniq
#merge all years into single file
#(cd tabledata/Zipf_2/uniq && seq 1 21 | xargs -P7 -n1 ../cutuniq.sh K320N93kAlpha1.1)
#remove the merged file all
#(cd tabledata/Zipf && rm all)
#sbt --error "runMain frontend.generators.SmallDatasetGenerator base"
#sbt --error "runMain frontend.generators.ZipfGenerator2 RMS"
sbt --error "runMain frontend.generators.SmallDatasetGenerator SMS"