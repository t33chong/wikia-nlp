import os, json
"""
Module WikiaSolr -- Wikia's scripts for working with Solr.
"""
from WikiaSolr.pageiditerator       import PageIdIterator
from WikiaSolr.etl                  import SolrETL
from WikiaSolr.queryiterator        import QueryIterator, DismaxQueryIterator
from WikiaSolr.groupedqueryiterator import GroupedQueryIterator
from WikiaSolr.backlinkgraph        import BacklinkGraph
from WikiaSolr.overseer             import *
from WikiaSolr.entities             import *
from WikiaSolr.freebase             import *
from WikiaSolr.domain               import WikiaDomainLoader

def get_config():
    return json.loads("".join(open(os.getcwd()+'/worker-config.json').readlines()))

VIDEO_WIKI_ID = 298117

