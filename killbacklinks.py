from WikiaSolr import QueryIterator, get_config
import json, requests

config = get_config()
r = range(0, 100)
r.reverse()
for i in r:
    q = QueryIterator(config, {u'query':u'wam:%d' % i, u'fields':u'id', 'sort':'wam desc'})
    update = []
    counter = 0
    for d in q:
        update.append({u'id':d[u'id'], 'outbound_links_txt':{'set':None}})
        counter += 1
        if len(update) >= 200:
            print counter,
            r = requests.post(config["common"]["solr_endpoint"] + "update/", \
                                  data=json.dumps(update), \
                                  headers={u'Content-type':'application/json'})
            print r.content
            update = []
