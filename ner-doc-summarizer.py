import re
import sys, json, urllib2, requests
from collections import defaultdict
from bs4 import BeautifulSoup
from nltk.tokenize.punkt import PunktSentenceTokenizer
from WikiaSolr import QueryIterator, get_config
from normalize import not_infobox
from time import time

start_time = time()

wid = sys.argv[1]
TOP_N = 5

def remove_newlines(text):
    if '\n' in text:
        return ' '.join([line for line in text.split('\n') if '.' in line])
    return text

qi = QueryIterator(get_config(), {'query': 'wid:%s AND confirmed_entities_txt:*' % wid, 'fields': 'id,url,html_en,confirmed_entities_txt'})

for doc in qi:
    entity_tally = defaultdict(int)
    confirmed_entities = [entity.lower() for entity in doc.get('confirmed_entities_txt', [])]
    html = urllib2.urlopen(doc['url']).read()
    soup = BeautifulSoup(html)
    text = ' '.join([p.text for p in soup.find_all('p')])
    sentences = filter(not_infobox, [remove_newlines(sentence) for sentence in PunktSentenceTokenizer().tokenize(text)])
    for (i, sentence) in enumerate(sentences):
        lowercase = sentence.lower()
        for entity in confirmed_entities:
            if entity in lowercase:
                entity_tally[i] += 1
    if not entity_tally:
        summary = ' '.join(sentences[:TOP_N]).encode('utf-8')
    else:
        entities_by_count = sorted(entity_tally.items(), key=lambda x:x[1], reverse=True)[:TOP_N]
        entities_by_id = sorted(entities_by_count, key=lambda x:x[0])
        summary = ' '.join([sentences[i] for (i, count) in entities_by_id]).encode('utf-8')
    print '---------------' + doc['id'] + '---------------'
    print doc['url']
    print '----------'
    print summary
    print '----------'
    for (i, sentence) in enumerate(sentences):
        print i, sentence.encode('utf-8')
    print '----------'
    print text.encode('utf-8')
#    print doc.get('html_en', '').encode('utf-8')
    print '------------------------------'
#    print 'entities', confirmed_entities
#    print 'sentences'
#    print 'tally', entity_tally
#    print 'entities by id'
#    for (i, count) in entities_by_id:
#        print i, count, sentences[i].encode('utf-8')

#print '\nCONFIRMED ENTITIES:'
#for entity, count in confirmed_entities_sorted[:200]:
#    print '%i\t%s' % (count, entity)

end_time = time()
print 'TOTAL TIME ELAPSED: %f SECONDS' % (end_time - start_time)
