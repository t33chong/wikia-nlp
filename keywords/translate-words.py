import fileinput

words = dict([(line.split('\t')[0], line.split('\t')[1][:-1]) for line in open('words.txt','r').readlines()])

for line in fileinput.input():
    line = line[:-1]
    count = ' '.join(line.split('\t')[0].split(' ')[:-1])
    translation = []
    grams = line.split('\t')
    for gram in grams:
        if gram.count(' '):
            translation.append(words[gram.split(' ')[-1]])
        else:
            translation.append(words[gram])
    print count, "\t".join(translation)
