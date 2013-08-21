# -*- coding: utf-8 *-*
'''
Given a wiki:
    grabs all page ids from the host,
    for each SERVICE:
        retrieves XML stub
        maps each page ID to a copy of the stub
        sends update request to solr
'''

import json
from WikiaSolr import SolrETL
from optparse import OptionParser

config = json.loads("".join(open('worker-config.json').readlines()))

parser = OptionParser()
parser.add_option("-w", "--wikihost", dest="wikihost", action="store", default=None,
                  help="Specifies the wiki host to access")
parser.add_option("-s", "--service", dest="service", action="store", default=None,
                  help="Specifies the service to invoke")
(options, args) = parser.parse_args()

options.workerType = 'wiki'

SolrETL(config, vars(options)).etlForWiki()
