import sys, json, requests, re
import time #test
from nltk.tokenize import PunktSentenceTokenizer
from nltk.corpus import stopwords
from WikiaSolr import QueryIterator, get_config, as_string, expand_entities, WikiaDomainLoader
from WikiaSolr.StanfordParser import ParserService
import codecs

wid = sys.argv[1]

qi = QueryIterator(get_config(), {'query': 'wid:%s AND iscontent:true' % wid, 'fields':'id,url,html_en' })
#qi = QueryIterator(get_config(), {'query': 'id:3125_199499', 'fields':'id,url,html_en' })
#qi = QueryIterator(get_config(), {'query': 'wid:%s AND iscontent:true' % wid, 'filterquery': 'views:[3500000 TO *]', 'fields':'id,url,html_en,views' }) #test

service = ParserService()

config = json.loads("".join(open('worker-config.json').readlines()))
host = config["common"]["solr_endpoint"]

entities = {}
confirmed_entities = {}

p = PunktSentenceTokenizer()

doc_count = 0

bullet1 = '\xe2\x80\xa2'.decode('utf-8')
bullet2 = '\xc2\xb7'.decode('utf-8')

start_time = time.time()

for doc in qi:
    print '========== %s ==========' % doc['id']
#    if doc_count < 4780:
#        continue
    text = as_string(doc.get('html_en', ''))
    #print text
    usable = ''
    for sentence in p.tokenize(text):
        #print sentence
        if len(sentence) < 500 and bullet1 not in sentence and bullet2 not in sentence:
            usable += '%s ' % sentence
    #print 'usable:', usable
    # avoid sending empty strings to parser
    if usable != '':
        parsed = service.parse(usable)
        #TODO - if cannot parse, skip stuff and jump straight to updating solr with dummy/blank values
        ne = []
        for n in parsed.getAllNamedEntities():
            ne.append(' '.join([w.word for w in n.getWords()]))
        np = parsed.getAllNodesOfType('NP')
        doc_entities = [e for e in list(set(ne + np)) if e.lower() not in stopwords.words('english')]
        url = WikiaDomainLoader(doc['url']).getUrl('wikia.php')
        expanded_entities = {}
        for i in range(0, len(doc_entities), 5):
            try:
                r = requests.get(url, params={'controller':'WikiaSearchController', 'method':'resolveEntities', 'entities':'|'.join(doc_entities[i:i+5])})
                j = json.loads(r.content)
            except:
                j = {}
            for k, v in j.items():
                if v != '':
                    expanded_entities.setdefault(k, v)
        
        entities[doc['id']] = list(set(doc_entities + expanded_entities.keys() + expanded_entities.values()))
        confirmed_entities[doc['id']] = list(set(expanded_entities.keys() + expanded_entities.values()))
    update_docs = [{'id':doc['id'], 'suspected_entities_txt':{'set':entities.get(doc['id'], [])}, 'confirmed_entities_txt':{'set':confirmed_entities.get(doc['id'], [])}}]
    requests.post(host+'update', data=json.dumps(update_docs), headers={u'Content-type':'application/json'})
    doc_count += 1
    print 'DOCS UPDATED: %i' % doc_count
    print 'TIME ELAPSED: %i seconds' % (time.time() - start_time)

print 'TOTAL TIME: %i seconds' % (time.time() - start_time)
