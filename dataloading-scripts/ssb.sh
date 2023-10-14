#!/bin/bash

sf=1 #100
mkdir -p tabledata/SSB/sf$sf/uniq

#Generate data
(cd ../ssb-dbgen && ./dbgen -v -s $sf && mv *.tbl ../sudokube/tabledata/SSB/sf$sf/ )

#Generate distinct values of each column
(cd tabledata/SSB/sf$sf/uniq && ../../mkAllUniq.sh)

#Split lineorder for parallel processing
(cd tabledata/SSB/sf$sf/ && ../split.sh $sf)

#Remove the original lineorder file
(cd tabledata/SSB/sf$sf/ && rm lineorder.tbl)

#Build Datacube
sbt --error  "runMain frontend.generators.SSBGen base"
sbt --error  "runMain frontend.generators.SSBGen RMS"
sbt --error  "runMain frontend.generators.SSBGen SMS"
