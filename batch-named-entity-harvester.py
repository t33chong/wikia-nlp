import os, sys, json, requests, re, tempfile, subprocess
import time #test
from pprint import pprint #test
from nltk.tokenize import PunktSentenceTokenizer
from nltk.corpus import stopwords
from WikiaSolr import QueryIterator, get_config, as_string, WikiaDomainLoader
from WikiaSolr.StanfordParser.parsedoutput import ParsedOutput
from corenlp import BatchParseThreader, parse_parser_xml_results

config = json.loads("".join(open('worker-config.json').readlines()))
host = config["common"]["solr_endpoint"]

def normalize(text):
    p = PunktSentenceTokenizer()
    bullet1 = '\xe2\x80\xa2'.decode('utf-8')
    bullet2 = '\xc2\xb7'.decode('utf-8')
    usable = ''
    for sentence in p.tokenize(text):
        if len(sentence) < 500:
            if bullet1 not in sentence and bullet2 not in sentence:
                usable += '%s ' % sentence
    return usable

class EntityBatch(object):
    def __init__(self, wid):
        self.wid = wid
        while True:
            try:
                self.url = WikiaDomainLoader(json.loads(requests.get('http://search-s11.prod.wikia.net:8983/solr/main/select', params={'q': 'wid:%s' % wid, 'fl': 'url', 'wt': 'json', 'rows': 1}).content)['response']['docs'][0]['url']).getUrl('wikia.php')
            except Exception as e:
                print e
                print 'retrying URL loading...'
                continue
            break
        self.tempdir = tempfile.mkdtemp()

    def get_url(self):
        return self.url

    def get_tempdir(self):
        return self.tempdir
        
    def write_batch_files(self):
        qi = QueryIterator(get_config(), {'query': 'wid:%s AND iscontent:true' % self.wid, 'fields':'id,url,html_en', 'sort': 'id asc'})

        doc_count = 0
        batch_count = 0

        for doc in qi:
            if doc_count % 100 == 0:
                batch_count += 1
                filepath = os.path.join(self.tempdir, str(batch_count))
                if not os.path.exists(filepath):
                    os.makedirs(filepath)
            text = normalize(as_string(doc.get('html_en', '')))
            if text != '':
                print 'writing %s to %s' % (doc['id'], filepath)
                output_file = open(os.path.join(filepath, doc['id']), 'w')
                output_file.write(text.encode('utf-8'))
                output_file.close()
            doc_count += 1

    def extract_entities(self, doc):
        pageid = doc.getFilename()
        print 'extracting entities from %s...' % pageid
        ne = [' '.join([w.word for w in n.getWords()]) for n in doc.getAllNamedEntities()]
        np = doc.getAllNodesOfType('NP')
        doc_entities = [e for e in list(set(ne + np)) if e.lower() not in stopwords.words('english')]
        expanded_entities = {}
        for i in range(0, len(doc_entities), 5):
            try:
                r = requests.get(self.url, params={'controller': 'WikiaSearchController', 'method': 'resolveEntities', 'entities':'|'.join(doc_entities[i:i+5])})
                j = json.loads(r.content)
            except:
                j = {}
            for k, v in j.items():
                if v != '':
                    expanded_entities.setdefault(k, v)
        #entities = list(set(doc_entities + expanded_entities.keys() + expanded_entities.values()))
        #confirmed_entities = list(set(expanded_entities.keys() + expanded_entities.values()))
        entities = list(set([re.sub(' \.', '', re.sub(' -[A-Za-z]{3}-', '', e).lower()) for e in doc_entities + expanded_entities.keys() + expanded_entities.values()]))
        confirmed_entities = list(set([re.sub(' \.', '', re.sub(' -[A-Za-z]{3}-', '', e).lower()) for e in expanded_entities.keys() + expanded_entities.values()]))
        return pageid, entities, confirmed_entities

def parse_xml(directory):
    for xmlfile in os.listdir(directory):
        file_name = re.sub('.xml$', '', os.path.basename(xmlfile))
        yield ParsedOutput(parse_parser_xml_results(open(os.path.join(directory, xmlfile), 'r').read(), file_name, raw_output=False))

def post_update(pageid, entities, confirmed_entities):
    update_docs = [{'id':pageid, 'suspected_entities_txt':{'set':entities}, 'confirmed_entities_txt':{'set':confirmed_entities}}]
    print 'posting', update_docs
    requests.post(host+'update', data=json.dumps(update_docs), headers={u'Content-type':'application/json'})

def main(wid):
    eb = EntityBatch(wid)
    eb.write_batch_files()
    bp = BatchParseThreader(xml_dir='/home/tristan/3490')
    xmldir = bp.parse(eb.get_tempdir(), num_threads=2)
    #xmldir = '/home/tristan/xml' # manually specifying dir to avoid re-parsing
    for doc in parse_xml(xmldir):
        pageid, entities, confirmed_entities = eb.extract_entities(doc)
        post_update(pageid, entities, confirmed_entities)

if __name__ == '__main__':
    wid = sys.argv[1]
    main(wid)
    #tempdir, url = write_batch_files(wid)
#    b = BatchParseThreader()
#    xmldir = b.parse(tempdir, num_threads=3)
#    for result in parse_xml(xmldir):
#        #pageid, entities, confirmed_entities = extract_entities(result)
#        post_update(extract_entities(result, url))
