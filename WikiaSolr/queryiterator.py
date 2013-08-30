import json, requests
"""
Given a query, iterate over all results.
Part of the WikiaSolr module.
"""
class QueryIterator(object):
    """ Options is a dictionary -- use vals(options) on an optparse instance """
    def __init__(self, config, options):
        if type(config) == dict:
            self.host = config["common"]["solr_endpoint"]
        else:
            self.host = config
        if not options.get('query', False):
            raise Exception("Query is required")
        self.configure(config, options)
        self.getMoreDocs()

    def configure(self, config, options):
        self.query = options.get('query')
        self.start = int(options.get('start', 0 ))
        self.firstStart = self.start
        self.rows = options.get('rows', 100)
        self.limit = options.get('limit', None)
        self.docs = []
        self.numFound = None
        self.at = 0
        self.fields = options.get('fields', '*')
        self.filterquery = options.get('filterquery', None)
        self.sort = options.get('sort', None)

    def __iter__(self):
        return self

    def percentLeft(self):
        start = self.firstStart
        max = self.numFound - start if not self.limit else self.limit
        return (float(self.at)/float(max)) * 100

    def getParams(self):
        params = {
             'q': self.query,
            'wt': 'json',
         'start': self.start,
          'rows': self.rows,
            'fl': self.fields
        }
        if self.filterquery:
            params['fq'] = self.filterquery
        if self.sort:
            params['sort'] = self.sort
        return params

    def getMoreDocs(self):
        if self.numFound is not None and self.start >= self.numFound:
            raise StopIteration
        #print 'requesting...'
        request = requests.get(self.host+"select", params=self.getParams(), timeout=300)
        #print request.content #DEBUG
        response = json.loads(request.content)
        self.numFound = response['response']['numFound']
        self.docs = response['response']['docs']
        self.start += self.rows
        return True

    def next(self):
        if self.at == self.limit:
            raise StopIteration
        if (len(self.docs) == 0):
            self.getMoreDocs()
        self.at += 1
        return self.docs.pop()

class DismaxQueryIterator(QueryIterator):
    def configure(self, config, options):
        super(DismaxQueryIterator, self).configure(config, options)
        self.defType = config.get('defType', 'edismax')
        self.queryFields = config.get('queryFields', 'nolang_txt')
        self.boostQuery = config.get('boostQuery', False)

    def getParams(self):
        params = super(DismaxQueryIterator, self).getParams()
        params['defType'] = self.defType
        params['qf'] = self.queryFields
        if self.boostQuery:
            params['bq'] = self.boostQuery
        return params
