import os
from collections import defaultdict
from operator import itemgetter

strings = defaultdict(int)

for filename in os.listdir('translated'):
    for line in open(os.path.join('translated', filename)).readlines():
        string = line.strip().split('\t')[1]
        strings[string] += 1

sort = sorted(strings.items(), key=itemgetter(1), reverse=True)

for string, count in sort[:50]:
    print '%i\t%s' % (count, string)
