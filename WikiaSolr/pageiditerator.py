import json, requests, os
from wikicities.DB import LoadBalancer
from WikiaSolr.domain import sanitizeUrl

"""
Grabs page IDs from the API
This isn't great scale and shouldn't be used in production
"""
class PageIdIterator(object):
    def __init__(self, host):
        self.host = sanitizeUrl(host)
        self.apfrom = None
        self.initDb()
        self.cursor = self.db.cursor()
        self.total = self.cursor.execute('SELECT page_id FROM page')
        self.stopAtEmpty = False
        self.at = 0
        

    def initDb(self):
        dbyml = os.environ.get('WIKIA_DB_YML', '/usr/wikia/conf/current/DB.yml')
        lb = LoadBalancer(dbyml)
        globalDb = lb.get_db_by_name('wikicities')
        cursor = globalDb.cursor()
        cursor.execute('SELECT city_dbname FROM city_list WHERE city_url = "%s"' % (self.host))
        result = cursor.fetchone()
        self.db = lb.get_db_by_name(result[0])

    def __iter__(self):
        return self

    def __len__(self):
        return self.total

    def next(self):
        result = self.cursor.fetchone()
        if not result:
            raise StopIteration()
        self.at += 1
        return result[0]

    def batch(self, group):
        try:
            return self[self.total:group]
        except:
            return []

    def count(self):
        return self.total

