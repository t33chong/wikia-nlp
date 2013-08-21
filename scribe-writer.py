# -*- coding: utf-8 *-*

import datetime, json, shutil, requests
from WikiaSolr import PageIdIterator
from optparse import OptionParser
from urlparse import urlparse


config = json.loads("".join(open('worker-config.json').readlines()))

parser = OptionParser()
parser.add_option("-w", "--wikihost", dest="wikihost", action="store", default=None,
                  help="If we specify this value instead of file, it allows us to reindex all pages on a wiki")
parser.add_option("-s", "--service", dest="service", action="store", default='All',
                  help="Specifies a single service to invoke on the end point. If omitted, calls all page-level services")
parser.add_option("-q", "--query", dest="query", action="store", default=None,
                  help="Allows you to queue a given solr query for reindexing (e.g. 'lang:pl', 'is_video:true', etc.)")
parser.add_option("-d", "--db", dest="dbconf", action="store", default="/usr/wikia/conf/current/DB.yml",
                  help="DB Config File")

(options, args) = parser.parse_args()

class BulkFile(object):

    def __init__(self, host):
        hn = urlparse(host).hostname
        self.host = hn if hn else host
        self.file = None
        self.fname = ''
        self.newFile()

    def newFile(self):
        self.cleanupFile()
        self.lines = 0
        self.fname = "/tmp/%s_%s" % (self.host, datetime.datetime.utcnow())
        self.file = open(self.fname, 'w')

    def cleanupFile(self):
        if self.file != None:
            self.file.close()
            shutil.move(self.fname, self.fname.replace('/tmp/', config["scribe"]["path"] + '/bulk/'))

    def write(self,line):
        self.lines += 1
        self.file.write(line)
        if self.lines >= 50000:
            self.newFile()

hostsToPageIds = {}

if hasattr(options, 'query') and options.query != None:
    endpoint = config["common"]["solr_endpoint"] + "select"
    myparams = {'q':options.query, 'fl':'host,pageid', 'rows': 0, 'start':0, 'wt':'json'}
    request = requests.get(endpoint, params=myparams)
    response = json.loads(request.content)
    myparams['rows'] = 500
    for i in range(0, response["response"]["numFound"], 500):
        myparams['start'] = i
        request = requests.get(endpoint, params=myparams)
        response = json.loads(request.content)
        for doc in response["response"]["docs"]:
            host = 'http://' + doc['host'] + '/'
            hostsToPageIds[host] = hostsToPageIds.get(host, []) + [doc['pageid']]
else:
    hostsToPageIds[options.wikihost] = PageIdIterator(options.wikihost)

for host in hostsToPageIds:
    bf = BulkFile(host)
    bf.write("\n".join([json.dumps({"serverName":host,"pageId":pageid,"services":[options.service]}) for pageid in hostsToPageIds[host]]))
    bf.cleanupFile()
