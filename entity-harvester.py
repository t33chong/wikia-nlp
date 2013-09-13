"""
Responsible for retrieving a list of top entities, given a wiki ID, and returning it to entity-harvester.

Wiki ID is provided as sys.argv[1]
"""

import sys
from subprocess import Popen
from nlp_rest_client import SolrWikiService, TopEntitiesService

def main(wid):
    response = TopEntitiesService().get(wid)[wid]
    entities = [entity[0] for entity in response]
    url = SolrWikiService().get(int(wid))[int(wid)]['url']
    print url + ',' + ','.join(entities)
    Popen('python %s' % os.path.join(os.getcwd(), 'nlp_rest_client/purge-cassandra.py'), shell=True)

if __name__ == '__main__':
    wid = sys.argv[1]
    main(wid)
