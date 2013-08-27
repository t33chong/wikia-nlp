from nltk.tokenize import PunktSentenceTokenizer

bullet1 = '\xe2\x80\xa2'.decode('utf-8')
bullet2 = '\xc2\xb7'.decode('utf-8')

p = PunktSentenceTokenizer()

def normalize(text):
    usable = ''
    for sentence in p.tokenize(text):
        if len(sentence) < 500:
            if bullet1 not in sentence and bullet2 not in sentence:
                usable += '%s ' % sentence
    return usable.encode('utf-8')

def not_infobox(text):
    if bullet1 in text or bullet2 in text:
        return False
    return True
