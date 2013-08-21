"""
Responsible for harvesting backlinks from outbound links on a per-wiki basis.

Wiki ID is provided as sys.argv[1]
Number of threads is optionally provided as sys.argv[2]
"""
from WikiaSolr import BacklinkGraph
import sys, json, requests

threads = 4 if len(sys.argv) < 3 else sys.argv[2]
config = json.loads("".join(open('worker-config.json').readlines()))
host = config["common"]["solr_endpoint"]
graph = BacklinkGraph(config, "wid:%s AND outbound_links_txt:*" % sys.argv[1], threads)
link_tuples = graph.backlinks
for offset in range(0, len(link_tuples), 200):
    update_docs = [{'id':lt[0], 'backlinks_txt':{'set':lt[1]}} for lt in link_tuples[offset:offset+200]]
    requests.post(host+'update', data=json.dumps(update_docs), headers={u'Content-type':'application/json'})
