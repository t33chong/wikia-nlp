import os
from nltk.tokenize import PunktSentenceTokenizer

def clean_list(text):
    bullet1 = '\xe2\x80\xa2'.decode('utf-8')
    bullet2 = '\xc2\xb7'.decode('utf-8')
    cleaned = []
    for sentence in PunktSentenceTokenizer().tokenize(text):
        if len(sentence.split(' ')) < 50:
            if bullet1 not in sentence and bullet2 not in sentence:
                usable.append(sentence.encode('utf-8'))
    return cleaned

def ensure_dir_exists(directory):
    """
    Makes sure the directory given as an argument exists, and returns the same
    directory.
    """
    if not os.path.exists(directory):
        os.makedirs(directory)
    return directory
