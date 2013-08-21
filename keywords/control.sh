#!/bin/sh

#FILES=/home/tristan/backend/bin/solr/keywords/wikis/*
FILES=/data/keywords/wikis/*
#echo $FILES;

for f in $FILES;
do
    #g=${f:(-1)};
    g=${f%.gz};
    h=${g##*/};
    ./create-ngrams.sh $h;
done;

zcat words.gz > words.txt;
python translator.py 50;
