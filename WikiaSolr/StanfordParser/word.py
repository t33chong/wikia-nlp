"""
Data access for words within a sentence within the Stanford CoreNLP parse response
"""
class Word:

    def __init__(self, data):
        [self.word, self.data] = data

    """ Retrieves Lemma for Word """
    def getLemma(self):
        return self.data.get(u'Lemma', self.word)

    """ Retrieves part of speech, using naive NN tag as backoff """
    def getPOS(self):
        return self.data.get(u'PartOfSpeech', u'NN')

    def getNamedEntityTag(self):
        return self.data.get(u'NamedEntityTag', u'O')
