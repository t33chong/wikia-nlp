import nltk
from word import Word
from namedentity import NamedEntity
"""
Data access for sentence-level information in Stanford Parser
"""
class Sentence:

    def __init__(self, dict):
        self.sentenceData = dict
        self.parseTree, self.namedEntities = None, None

    """ Returns NLTK-style parse trees """
    def getParsed(self):
        if not self.parseTree:
            self.parseTree = nltk.tree.Tree.parse(self.sentenceData[u'parsetree'])
        return self.parseTree

    """ Returns parse tree in raw string format """
    def getTreeString(self):
        return self.sentenceData[u'parsetree']

    """ Returns the raw text value of the given sentence """
    def getText(self):
        return self.sentenceData[u'text']

    """ Returns a list of Word instances """
    def getWords(self):
        return [Word(w) for w in self.sentenceData[u'words']]

    """ Presently returns the list of dependencies, but we should probably add class abstraction here """
    def getDependencies(self):
        return self.sentenceData[u'dependencies']

    """ Retrieves named entities that actually receieved a tag from the parser """
    def getNamedEntities(self):
        if not self.namedEntities:
            entities = []
            currwords = []
            currtag = 'O'
            words = self.getWords()
            for i in range(0, len(words)):
                word = words[i]
                newtag = word.getNamedEntityTag()
                if newtag != 'O':
                    currwords += [word]
                if len(currwords) > 0 and (currtag != newtag or i == len(words) - 1):
                    entities += [NamedEntity(currwords, currtag)]
                    currwords = []
                currtag = newtag
            self.namedEntities = entities
        return self.namedEntities
