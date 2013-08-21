import re, json, requests

def extract_category_text(category):
    category = re.sub(' ', '%20', category)
    #r = requests.get('http://muppet.wikia.com/wikia.php?controller=ArticlesApiController&method=getTop&category=Muppet%20Movies')
    r = requests.get('http://muppet.wikia.com/wikia.php?controller=ArticlesApiController&method=getTop&category=%s' % category)
    j = json.loads(r.content)
    text = ''
    for item in j['items']:
        id_ = '831_%i' % item['id']
        item_request = requests.get('http://search-s11.prod.wikia.net:8983/solr/main/select', params={'q': 'id:%s' % id_, 'fl': 'html_en', 'wt': 'json'})
        item_json = json.loads(item_request.content)
        html_en = item_json['response']['docs'][0].get('html_en', '')
        text += html_en
    return text

def create_sentence_list(text):
    from nltk.tokenize import PunktSentenceTokenizer
    p = PunktSentenceTokenizer()
    return [sentence for sentence in p.tokenize(text)]

def lexrank(text):
    from summarizer.lexrank import gen_lexrank_summary
    #print gen_lexrank_summary(sentences, 100)
    return ' '.join(gen_lexrank_summary(create_sentence_list(text), 100))

def sumtract(text):
    from sumtract.SumBasic import easy_multi_summarize
    return easy_multi_summarize(text)

def sumtract_twitter(text):
    from sumtract.SumBasic_tweet import easy_summarize
    return easy_summarize(text, N=255)

def simple(text):
    from summarize.summarize import SimpleSummarizer
    return SimpleSummarizer().summarize(text, 3)

#def rotten(text):
#    import re
#    from rotten.summarize import summarize_block
#    print re.sub('\s+', ' ', summarize_block(text)).strip()

def main():
    #text = extract_category_text('Muppet Movies').encode('utf-8')
    text = extract_category_text('Muppet Show Sketches').encode('utf-8')
    print 'LEXRANK:'
    print lexrank(text)
    print '\nSUMBASIC:'
    print sumtract(text)
    print '\nSUMBASIC TWITTER:' 
    print sumtract_twitter(text)
    print '\nSIMPLE SUMMARIZER:'
    print simple(text)
    #rotten(text)

if __name__ == '__main__':
    main()
