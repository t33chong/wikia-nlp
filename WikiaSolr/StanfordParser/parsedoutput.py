import nltk
from sentence import Sentence

"""
Provides an API for interacting with the output of the Stanford NLP Parser
"""
class ParsedOutput:
    #def __init__(self, dictionary, batch=False):
    def __init__(self, dictionary):
        #self.batch = batch
        self.dictionary = dictionary

    def getFilename(self):
        """ Returns the filename """
        return self.dictionary.get(u'file_name')

    def getSentences(self):
        """ Returns a list of each ParsedSentence """
#        if self.batch ==True:
#            try:
#                return [BSentence(s) for s in self.dictionary.get('sentences')]
#            except:
#                return []
        try:
            return [Sentence(s) for s in self.dictionary.get(u'sentences')]
        except:
            return []

    def getAllNodesOfType(self, nodetype):
        """ Returns each span of text that is a node of the passed value (e.g. NP) """
        nodes = []
        for tree in self.getAllParseTrees():
            for subtree in tree.subtrees():
                if subtree.node == nodetype:
                    nodes.append(' '.join(subtree.leaves()))
        return nodes

    def getAllWords(self):
        """ Returns the ParsedWord instances words in each ParsedSentence """
        words = []
        for sentence in self.getSentences():
            for word in sentence.getWords():
                words.append(word)
        return words

    def getAllParseTrees(self):
        """ Returns a list of NLTK parsed trees from the sentences in the output"""
        return [s.getParsed() for s in self.getSentences()]

    def getAllTreeStrings(self):
        """ Returns a list of parse trees in raw string format """
        return [s.getTreeString() for s in self.getSentences()]

    def getAllNamedEntities(self):
        """ Returns a list of named entities """
        entities = []
        for sentence in self.getSentences():
            for entity in sentence.getNamedEntities():
                entities.append(entity)
        return entities
