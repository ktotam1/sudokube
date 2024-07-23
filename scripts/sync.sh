#!/bin/bash

dest="datadell"
echo $dest


rsync -avz --exclude-from='.gitignore' --progress --delete src/ $dest:/home/alex/sudokube/sudokube/src
rsync -avz --exclude-from='.gitignore' --progress --delete example-data/ $dest:/home/alex/sudokube/sudokube/example-data
rsync -avz --exclude-from='.gitignore' --progress build.sbt $dest:/home/alex/sudokube/sudokube/build.sbt
rsync -avz --exclude-from='.gitignore' --progress project/ $dest:/home/alex/sudokube/sudokube/project/
rsync -avz --exclude-from='.gitignore' --progress scripts/ $dest:/home/alex/sudokube/sudokube/scripts/
rsync -avz --exclude-from='.gitignore' --progress hosts $dest:/home/alex/sudokube/sudokube/
