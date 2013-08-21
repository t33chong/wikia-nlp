#!/bin/sh

cat $1 | sort | uniq -c | sort -nr |  head -n$2 | python translate-words.py | egrep -v "[:punct:]"
