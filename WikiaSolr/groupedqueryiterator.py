import json, requests
"""
Allows us to iterate over groupings.
Original plan is to use this to iterate 
over wikis by WAM descending.
"""
class GroupedQueryIterator(object):
    def __init__(self, config, options):
        if type(config) == dict:
            self.host = config["common"]["solr_endpoint"]
        else:
            self.host = config
        if not options.get('query', False):
            raise Exception("Query is required")
        self.query = options['query']
        if not options.get('groupField', False):
            raise Exception("Grouping field is required")
        self.groupField = options['groupField']
        self.groupRows = options.get('groupRows', 1)
        self.start = 0
        self.rows = options.get('rows', 100)
        self.fields = options.get('fields', '*')
        self.filterQuery = options.get('filterQuery', '')
        self.groups = []
        self.sort = options.get('sort', 'score desc')
        self.groupsFound = None

    def __iter__(self):
        return self

    def getMoreGroups(self):
        if self.groupsFound is not None and self.groupsFound <= self.start:
            raise StopIteration
        params = {
                    'q':self.query,
                 'rows':self.rows,
                'start':self.start,
                'group':'true',
               'fl':self.fields,
          'group.field':self.groupField,
          'group.limit':self.groupRows,
        'group.ngroups':'true',
                 'sort':self.sort,
                   'wt':'json'
        }
        if self.filterQuery:
            params['fq'] = self.filterQuery
        try:
            response = json.loads(requests.get(self.host+'select', params=params).content)
            self.groupsFound = response["grouped"][self.groupField]["ngroups"]
            self.groups = response["grouped"][self.groupField]["groups"]
        except:
            print response
            #raise TypeError
            self.groupsFound = 0
            self.groups = {}

    def next(self):
        if len(self.groups) == 0:
            self.getMoreGroups()
        return self.groups.pop()
