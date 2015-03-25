#!/bin/bash
wget -N http://snap.stanford.edu/data/soc-LiveJournal1.txt.gz
mkdir -p live_journal
mv soc-LiveJournal1.txt.gz live_journal/part-r-00000.gz

python pack-bin.py live_journal
rm live_journal/part-r-00000.gz
