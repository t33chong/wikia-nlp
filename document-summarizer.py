import os, sys, json, requests, re, tempfile, subprocess
from normalize import normalize
from nltk.tokenize import PunktSentenceTokenizer
from nltk.corpus import stopwords
from WikiaSolr import QueryIterator, get_config, as_string, WikiaDomainLoader
from WikiaSolr.StanfordParser.parserservice import ParserService
from WikiaSolr.StanfordParser.parsedoutput import ParsedOutput
from corenlp import BatchParseThreader, parse_parser_xml_results
from sumtract.SumBasic_tweet import twitterize, easy_summarize as sumbasic
from sumtract.twitter_simplify import easy_simplify
from summarize.summarize import SimpleSummarizer
from rotten.summarize import summarize_block

#def summarize_single_documents(wid):
#    """ Uses sumtract.SumBasic_tweet """
#    qi = QueryIterator(get_config(), {'query': 'wid:%s AND iscontent:true' % wid, 'fields':'id,url,html_en' })
#
#    for doc in qi:
#        text = as_string(doc.get('html_en', ''))
#        usable = normalize(text)
#        print doc['id'], easy_summarize(usable)

#TODO: remove eval() in favor of a more efficient function call

def summarize_muppets(function):
    print function
    while True:
        try:
            top_request = requests.get('http://muppet.wikia.com/wikia.php', params={'controller': 'ArticlesApiController', 'method': 'getTop'})
        except:
            continue
        break
    top = json.loads(top_request.content)
    pageids = [item['id'] for item in top['items']]
    for pageid in pageids:
        while True:
            try:
                doc_request = requests.get('http://search-s11.prod.wikia.net:8983/solr/main/select', params={'q': 'id:831_%i' % pageid, 'fl': 'id,url,html_en', 'wt': 'json'})
            except:
                continue
            break
        doc = json.loads(doc_request.content)
        try:
            id_ = doc['response']['docs'][0].get('id')
            url = doc['response']['docs'][0].get('url')
            html_en = doc['response']['docs'][0].get('html_en', '')
        except IndexError:
            continue
        
        print id_, url
        print '----------'
        print eval('%s(normalize(html_en))' % function)
        print '----------'
        print html_en.encode('utf-8')
        print '----------------------------------------'

def first_sentence(text):
    from nltk.tokenize import PunktSentenceTokenizer

    p = PunktSentenceTokenizer()
    return twitterize(p.tokenize(text)[0])

def simplified(text):
    first = first_sentence(text)
    try:
        parsed = ParserService().parse(first).getAllTreeStrings()[0]
    except IndexError:
        return ''
    return twitterize(easy_simplify(parsed))

def no_parens(text):
    return twitterize(first_sentence(text))

def simple_summarize(text):
    return twitterize(SimpleSummarizer().summarize(text, 1))

def rotten_summarize(text):
    return twitterize(re.sub('\s+', ' ', summarize_block(text)).strip())

#    summaries = map(lambda p: re.sub('\s+', ' ', summarize_block(p.text)).strip(), b.find_all('p'))
#    summaries = sorted(set(summaries), key=summaries.index) #dedpulicate and preserve order
#    summaries = [ re.sub('\s+', ' ', summary.strip()) for summary in summaries if filter(lambda c: c.lower() in string.letters, summary) ]
#    #return Summary(url, b, html.title.text if html.title else None, summaries)
#    return Summary(url, b, html.title.text.encode('utf-8') if html.title else None, summaries)

def main(function):
    summarize_muppets(function)

if __name__ == '__main__':
    #wid = sys.argv[1]
    function = sys.argv[1]
    main(function)
