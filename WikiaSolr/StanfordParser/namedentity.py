"""
Data Abstraction for Named Entities
"""
class NamedEntity:
    def __init__(self, words, tag):
        self.words, self.tag = words, tag

    def getTag(self):
        return self.tag

    def getWords(self):
        return self.words
