# -*- coding: utf-8 *-*
"""
Given a file that lists hosts and page IDs in JSON objects:
    Groups all values in the file by host
    Requests batches of XML from the application
    Sends that XML to Solr, in batches
"""

import json, os
from optparse import OptionParser
from WikiaSolr import SolrETL
from datetime import datetime, timedelta

startTime = datetime.now()

config = json.loads("".join(open('worker-config.json').readlines()))

parser = OptionParser()
parser.add_option("-f", "--file", dest="file", action="store", default=None,
                  help="Specifies the file to attach to this worker")
parser.add_option("-w", "--wikihost", dest="wikihost", action="store", default=None,
                  help="If we specify this value instead of file, it allows us to reindex all pages on a wiki")
parser.add_option("-s", "--service", dest="service", action="store", default=None,
                  help="Specifies a single service to invoke on the end point. If omitted, calls all page-level services")
parser.add_option("-g", "--serviceGroup", dest="serviceGroup", action="store", default=None,
                  help="Specifies a group of services to invoke on the end point. Overrides individual service param.")
parser.add_option("-i", "--wiki-id", dest="wikiId", action="store", default=None,
                  help="Allows us to reindex a wiki by its id")
(options, args) = parser.parse_args()

options.workerType = 'page'

SolrETL(config, vars(options)).etlForPages()

if options.file and os.path.isfile(options.file):
    os.remove(options.file)

print 'finished in', (datetime.now() - startTime).seconds, 'seconds'
