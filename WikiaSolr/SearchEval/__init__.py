# -*- coding: utf-8 *-*
import requests, json, collections, random, sys
from urllib2 import unquote as urldecode

requests.encoding = 'utf-8'

def getPageIdsForTitles(host, titles):
    if len(titles) == 0:
        return []
    url = "http://%s/api.php" % host
    query = {
        "action": "query",
        "titles": u'|'.join(titles),
     "redirects": "1",
        "format": "json"
        }
    r = requests.get(url, params=query)
    if r.status_code == 200 and r.content:
        try:
            response = json.loads(r.content)
            return [page for page in response["query"]["pages"].keys()]
        except:
            pass
    return []

class SearchQuery:

    def __init__(self, host, term):
        self.host = host
        self.url = "http://%s/wiki/Special:Search" % host
        self.term = term
        self.results = []
        self.broken = False
        self.execute()

    def execute(self):
        params = {
            'search': self.term,
          'fulltext': 'search',
            'format': 'json',
        'jsonfields': 'title,pageid',
              'rand': random.randint(0, 999999999),
             'limit': 5
        }
        r = requests.get(self.url, params=params)

        if r.status_code == 200 and r.content:
            try:
                results = json.loads(r.content)
            except:
                print r.content, sys.exc_info()
                results = {}
                print "problem with ",self.url, r.url
                self.broken = True
        self.results = [result.get('title', '') for result in results]
        self.resultIds = [result.get('pageid', 0) for result in results]

    def getResults(self):
        return self.results

    def getResultIds(self):
        return self.resultIds


class ResultSetComparer:

    def __init__(self, host, term, expected):
        self.term = term.strip()
        self.host = host.strip()
        self.expected = expected
        self.expectedIds = getPageIdsForTitles(host, expected)
        self.actual = []
        self.score = 0

    def compareResults(self, verbose=False):
        if not self.term or not self.host:
            return 1
        if verbose:
            print "Querying for \"%s\" on %s" % (self.term, self.host)
        query = SearchQuery(self.host, self.term)
        if query.broken:
            return 1
        self.actual = query.getResults()
        self.actualIds = query.getResultIds()
        # assign a point for each value that is found in the top 5 results
        """
        self.score += abs(len(self.expectedIds) - 3) # for those that don't have 3 results
        self.score += len([i for i in self.expectedIds if i in self.actualIds])
        scorings = [3, 2, 1]
        for i in range(0, len(scorings)):
            if len(self.expectedIds) <= i or len(self.actualIds) <= i or self.expectedIds[i] <= 0 or not self.expectedIds[i]:
                self.score += scorings[i] # mulligan
            elif self.expectedIds[i] == self.actualIds[i]:
                self.score += scorings[i]
        """
        self.score += abs(len(self.expected) - 3) # for those that don't have 3 results
        self.score += len([i for i in map( lambda x: x.lower(), self.expected ) if i in map( lambda x: x.lower(), self.actual )] )
        scorings = [3, 2, 1]
        for i in range(0, len(scorings)):
            if len(self.expected) <= i or len(self.actual) <= i or not self.expected[i] or self.expected[i] == '-':
                self.score += scorings[i] # mulligan
            elif self.expected[i].lower() == self.actual[i].lower():
                self.score += scorings[i] # assign score
            elif len(self.expected) > i and len(self.actual) > i+1 and self.expected[i].lower == self.actual[i+1].lower():
                self.score += scorings[i] - 1 #reduced score for being one place off
            elif i > 0 and self.expected[i].lower() == self.actual[i-1].lower():
                self.score += scorings[i] - 1 #reducred score for being one place off
        return self.score

    def getRealResults(self):
        return self.actual

    def getExpectedResults(self):
        return self.expected

    def getHost(self):
        return self.host

    def getTerm(self):
        return self.term

    def getScore(self):
        return self.score

class CsvResultSetComparerGroup:
    def __init__(self, filename):
        self.testCases = [unicode(line, 'utf-8').split(u',') for line in open(filename).readlines()[2:]]
        
    def runTests(self, verbose=False):
        self.comparisons = [ResultSetComparer(line[0], line[1], line[2:4]) for line in self.testCases]
        self.scores = map(lambda comp: comp.compareResults(verbose=verbose), self.comparisons)
    
    def totalScore(self):
        return sum(self.scores)/(len(self.comparisons) * 9) #9 is max possible

    def averageScore(self):
        return sum(self.scores)/len(self.scores)

    def textComparison(self):
        string = u'';
        string += u'Total Score:\t%.2f/%d\nAverage Score:\t%.2f\n\n' % (sum(self.scores), len(self.comparisons) * 9, self.averageScore())
        for i in range(0, len(self.comparisons)):
            case = self.comparisons[i]
            string += u'='*25
            string += u'\nHost:\t' + case.getHost() + u'\n'
            string += u'Term:\t' + case.getTerm() + u'\n'
            string += u'Score:\t%.2f\n' % case.getScore()
            string += u'Expect:\t' + u', '.join(case.getExpectedResults()) + u'\n'
            string += u'\t%s\n' % u', '.join(case.expectedIds)
            string += u'Actual:\t' + u', '.join(case.getRealResults()) + u'\n'
            string += u'\t%s\n' % u', '.join([str(i) for i in case.actualIds])
            string += u'\n' + u'='*25 + u'\n'
        return string

    def csvComparison(self):
        string = u'';
        string += u'Total Score:,%.2f/%d,Average Score:\t%.2f\n' % (sum(self.scores), len(self.comparisons) * 9, self.averageScore())
        string += u'Host,Term,Score,Type,1,2,3\n'
        for i in range(0, len(self.comparisons)):
            case = self.comparisons[i]
            string += u'%s,%s,%s\n' % (case.getHost(), case.getTerm(), case.getScore())
            string += u',,,Expected:,%s\n' % u','.join([i.replace(u'\n', u'').replace(u'"', u'') for i in case.getExpectedResults()])
            string += u',,,Actual:,%s\n' % u','.join([i.replace(u'\n', u'').replace(u'"', u'') for i in case.getRealResults()])
        return string
