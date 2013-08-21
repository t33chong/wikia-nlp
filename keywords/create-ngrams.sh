#!/bin/sh

mkdir -p temp;
#zcat $1/* > temp/$1.all;
zcat /data/keywords/wikis/$1.gz | grep -Pv '^\t' > temp/$1.all;

for i in $(seq 4);
do
    tail -n+$i temp/$1.all > temp/$1.$i;
    #echo $i;
done;

mkdir -p ngrams;
#cat temp/$1.1 > ngrams/$1.1grams;
#paste temp/$1.1 temp/$1.2 | head -n-1 > ngrams/$1.2grams;
#paste temp/$1.1 temp/$1.2 temp/$1.3 | head -n-2 > ngrams/$1.3grams;
#paste temp/$1.1 temp/$1.2 temp/$1.3 temp/$1.4 | head -n-3 > ngrams/$1.4grams;
cat temp/$1.1 | sort | uniq -c | sort -nr | head -n5000 > ngrams/$1.1grams;
paste temp/$1.1 temp/$1.2 | head -n-1 | sort | uniq -c | sort -nr | head -n5000 > ngrams/$1.2grams;
paste temp/$1.1 temp/$1.2 temp/$1.3 | head -n-2 | sort | uniq -c | sort -nr | head -n5000 > ngrams/$1.3grams;
paste temp/$1.1 temp/$1.2 temp/$1.3 temp/$1.4 | head -n-3 | sort | uniq -c | sort -nr | head -n5000 > ngrams/$1.4grams;

rm temp/$1.*;
