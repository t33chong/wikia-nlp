from WikiaSolr import QueryIterator, get_config
import json, requests, sys

config = get_config()
q = QueryIterator(config, {u'query':u'wid:%s AND outbound_links_txt:*' % sys.argv[1], u'fields':u'id'})
update = []
counter = 0
for d in q:
    update.append({u'id':d[u'id'], 'outbound_links_txt':{'set':None}})
    counter += 1
    if len(update) >= 500:
        r = requests.post(config["common"]["solr_endpoint"] + "update/?commit=true", \
                          data=json.dumps(update), \
                          headers={u'Content-type':'application/json'})
        update = []
