# -*- coding: utf-8 *-*

from WikiaSolr import SolrETL
from optparse import OptionParser
import pymongo, json

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
(options, args) = parser.parse_args()

options.useScribe = True
options.workerType = 'page'
etl = SolrETL(config, vars(options))
etl.etlFromMongo()
