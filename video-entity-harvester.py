import os, sys, json, requests, re, tempfile, subprocess
import time
from pprint import pprint #test
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
from WikiaSolr import QueryIterator, get_config, as_string
from WikiaSolr.StanfordParser.parsedoutput import ParsedOutput
from corenlp import BatchParseThreader, Subdir, parse_parser_xml_results

config = json.loads("".join(open('worker-config.json').readlines()))
host = config["common"]["solr_endpoint"]

def write_batch_files():
    qi = QueryIterator(get_config(), {'query': "wid:298117 AND -title_en:\"File:IGN Weekly 'Wood\" is_video:true", 'fields': 'id,title_en,video_description_txt,video_keywords_txt,video_actors_txt,video_tags_txt,video_genres_txt'})

    doc_count = 0
    batch_count = 0

    #tempdir = tempfile.mkdtemp()
    tempdir = '/home/tristan/temp/video'

    for doc in qi:
        if doc_count % 100 == 0:
            batch_count += 1
            filepath = os.path.join(tempdir, str(batch_count))
            if not os.path.exists(filepath):
                os.makedirs(filepath)
        #no_parse = []
        #for field in [u'video_keywords_txt', u'video_tags_txt', u'video_actors_txt', u'video_genres_txt']:
        #    for tag in doc.get(field, []):
        #        no_parse.append(tag)
        #text = '\t'.join(list(set(no_parse))) + '\n'
        #for field in [u'title_en', u'video_description_txt']:
        #    val = doc.get(field, None)
        #    if val:
        #        text += as_string(val)
        fields = []
        for field in doc:
            if field != u'id':
                val = doc.get(field, None)
                if val:
                    fields.append(as_string(doc[field]))
        text = '.\n'.join(fields)
        print text
        output_file = open(os.path.join(filepath, doc[u'id']), 'w')
        output_file.write(text.encode('utf-8'))
        output_file.close()
        doc_count += 1
    return tempdir

#
#        text = as_string(doc.get('html_en', ''))
#        usable = ''
#        for sentence in p.tokenize(text):
#            if len(sentence) < 500 and bullet1 not in sentence and bullet2 not in sentence:
#                usable += '%s ' % sentence
#        if usable != '':
#            print 'writing %s to %s' % (doc['id'], filepath)
#            output_file = open(os.path.join(filepath, doc['id']), 'w')
#            output_file.write(usable.encode('utf-8'))
#            output_file.close()
#        doc_count += 1
#    return tempdir, url

def parse_xml(directory):
    for xmlfile in os.listdir(directory):
        file_name = re.sub('.xml$', '', os.path.basename(xmlfile))
        #print 'parsing...'
        #print parse_parser_xml_results(open(os.path.join(directory, xmlfile), 'r').read(), file_name, raw_output=False)
        print file_name
        yield ParsedOutput(parse_parser_xml_results(open(os.path.join(directory, xmlfile), 'r').read(), file_name, raw_output=False))

def extract_entities(doc):
    print 'extracting entities from %s...' % doc.getFilename()
    nps = list(set([re.sub(' \.', '', re.sub(' -[A-Z]{3}-', '', np).lower()) for np in doc.getAllNodesOfType('NP')]))
    p = PorterStemmer()
    entities = []
    for np in nps:
        try:
            response = json.loads(requests.get(host+'select', params={'q': 'wam:[50 TO 100] AND iscontent:true AND lang:en AND (title_en:"%s" OR redirect_titles_mv_en:"%s")' % (np, np), 'fl': 'title_en,redirect_titles_mv_en', 'wt': 'json'}).content)
        except requests.exceptions.ConnectionError:
            while True:
                time.sleep(15)
                print 'retrying connection...'
                try:
                    response = json.loads(requests.get(host+'select', params={'q': 'wam:[50 TO 100] AND iscontent:true AND lang:en AND (title_en:"%s" OR redirect_titles_mv_en:"%s")' % (np, np), 'fl': 'title_en,redirect_titles_mv_en', 'wt': 'json'}).content)
                    break
                except requests.exceptions.ConnectionError:
                    continue
        docs = response[u'response'][u'docs']
        if len(docs) > 0:
            titles = [docs[0][u'title_en']] + docs[0].get(u'redirect_titles_mv_en', [])
        else:
            titles = []
        if len(titles) > 0:
            titles = [' '.join([p.stem(w.lower()) for w in t.split(' ')]) for t in titles]
        stem_np = ' '.join([p.stem(w) for w in np.split(' ')])
        for title in titles:
            if stem_np == title:
                entities.append(np)
                print np
                break
    #print doc.getFilename(), entities
    return (doc.getFilename(), entities)

def post_update(pageid, confirmed_entities):
    update_docs = [{'id':pageid, 'confirmed_entities_txt':{'set':confirmed_entities}}]
    print 'posting update:', update_docs
    try:
        requests.post(host+'update', data=json.dumps(update_docs), headers={u'Content-type':'application/json'})
    except requests.exceptions.ConnectionError:
        while True:
            time.sleep(15)
            print 'retrying connection...'
            try:
                requests.post(host+'update', data=json.dumps(update_docs), headers={u'Content-type':'application/json'})
                break
            except requests.exceptions.ConnectionError:
                continue


if __name__ == '__main__':
    ##tempdir = write_batch_files()
    ##print tempdir
    #tempdir = '/home/tristan/temp/video' #manual
    #start_time = time.time()
    #b = BatchParseThreader(xml_dir='/home/tristan/xmlvideo')
    #xmldir = b.parse(tempdir, num_threads=2)
    xmldir = '/home/tristan/xmlvideo'
    #xmldir = '/home/tristan/xmlvideo-debug'

    for result in parse_xml(xmldir):
        pageid, entities = extract_entities(result)
        #print 'pageid:', pageid
        #print 'entities:', entities
        #post_update(extract_entities(result))
        post_update(pageid, entities)

    #print 'TIME ELAPSED: %i seconds' % (time.time() - start_time)
#    for result in parse_xml(xmldir):
#        #pageid, entities, confirmed_entities = extract_entities(result)
#        post_update(extract_entities(result, url))
