import os, sys, time, subprocess, json, requests, uuid, gzip
from operator import itemgetter
from nltk import ngrams
from nltk.corpus import stopwords
from nltk.tokenize.regexp import WordPunctTokenizer
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.stem.porter import PorterStemmer
from WikiaSolr import QueryIterator, get_config

def is_useful(string):
    for word in string.split(' '):
        if not word.isalpha():
            return False
        if word in stopwords.words('english'):
            return False
    return True

class WikiNgramDB(object):
    def __init__(self):
        self.words_file = 'keywords/words.gz'
        self.wiki_filepath = '/data/keywords/wikis'
        if not os.path.exists(self.wiki_filepath):
            os.makedirs(self.wiki_filepath)
        self._load_data()

    def _load_data(self):
        self.counter = 0
        self.words = {}
        if os.path.exists(self.words_file):
            words_file = gzip.open(self.words_file, 'r')
            while True:
                try:
                    for line in words_file:
                        try:
                            id, token = line.strip('\n').split('\t')
                            self.words[token] = int(id)
                            self.counter = int(id)
                        except:
                            pass
                    words_file.close()
                except IOError:
                    words_file.close()
                    subprocess.Popen('gunzip < %s | gzip > %s' % (self.words_file, self.words_file), shell=True)
                    continue
                break
        self.counter += 1

    def extract_words(self, wid):
        """Updates db with previously unseen words and lemmas, and page unigrams"""
        words_file = gzip.open(self.words_file, 'a')
        page_file = gzip.open(os.path.join(self.wiki_filepath, '%i.gz' % wid), 'w')
        w = WordPunctTokenizer()
        qi = QueryIterator(get_config(), {'query': 'wid:%s AND iscontent:true' % str(wid), 'fields': 'id, wid, pageid, html_en', 'sort': 'id asc'})
        print 'starting extraction for wiki %s...' % str(wid)
        for doc in qi:
            print 'extracting words for %s...' % doc['id']
            page_file.write('\t%s\n' % doc['pageid'])
            for word in w.tokenize(doc.get('html_en', '').lower()):
                if word not in self.words:
                    self.words[word] = self.counter
                    words_file.write('%i\t%s\n' % (self.counter, word.encode('utf-8')))
                    self.counter += 1
                page_file.write('%i\n' % self.words.get(word, 0))
        page_file.close()
        words_file.close()

def main():
    start = time.time()
    n = WikiNgramDB()
    #print n.counter
    input_file = sys.argv[1]
    for line in open(input_file):
        n.extract_words(int(line.strip()))
        print 'TIME ELAPSED: %i seconds' % (time.time() - start)
    end = time.time()
    print 'TOTAL TIME ELAPSED: %i seconds' % (end - start)

if __name__ == '__main__':
    main()
