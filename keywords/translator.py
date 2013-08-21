import os, sys, re, gzip
from nltk.corpus import stopwords

limit = int(sys.argv[1])

print 'STARTING TRANSLATION...'

#words = dict([(line.split('\t')[0], line.split('\t')[1][:-1]) for line in gzip.open('words.gz','r')])
words = dict([(line.split('\t')[0], line.split('\t')[1][:-1]) for line in open('words.txt','r')])

def is_useful(text):
    if re.search('[^a-z ]', text):
        return False
    for word in text.split(' '):
        if word in stopwords.words('english'):
            return False
    return True

if not os.path.exists('translated'):
    os.makedirs('translated')

for filename in os.listdir('ngrams'):
    match_count = 0
    out = open(os.path.join('translated', filename), 'w')
    for line in open(os.path.join('ngrams', filename)):
        if match_count >= limit:
            break
        line = line[:-1]
        count = ' '.join(line.split('\t')[0].split(' ')[:-1])
        translation = []
        grams = line.split('\t')
        for gram in grams:
            if gram.count(' '):
                translation.append(words[gram.split(' ')[-1]])
            else:
                translation.append(words[gram])
        translated = ' '.join(translation)
        if is_useful(translated):
            match_count += 1
            out.write('%s\t%s\n' % (count, translated))
            print count, translated
    out.close()
