"""
This process is responsible for updating the videos for a given wiki with their views.

Wiki ID is provided as sys.argv[1]
"""
import sys, json, requests, itertools

wid = sys.argv[1]
threads = 4 if len(sys.argv) < 3 else sys.argv[2]
config = json.loads("".join(open('worker-config.json').readlines()))
host = config["common"]["solr_endpoint"]

"""
Access the VideoViews service for a set of documents on the same host
"""
def get_video_views(docs):
    host, ids = docs[0]['host'], '|'.join([str(doc['pageid']) for doc in docs])
    params = {'controller':'WikiaSearchIndexer', 'method':'get', 'service':'MediaData', 'ids':ids, 'format':'json'}
    try:
        return json.loads(requests.get("http://%s/wikia.php" % (host), params=params).content)['contents']
    except:
        return []


params = {'q':'wid:%s AND is_video:true' % wid, 'rows':100, 'start':0, 'format':'json', 'fl':'pageid,host', 'wt':'json'}
while True:
    response = json.loads(requests.get(host+'select', params=params).content)
    if len(response['response']['docs']) > 0:
        docs = response['response']['docs']
        views = list(itertools.chain([get_video_views(docs[x:x+15]) for x in xrange(0, len(docs), 15)]))[0]
        try:
            requests.post(host+'update?commit=true', data=json.dumps(views), headers={u'Content-type':'application/json'}).content
        except:
            pass
    if response['response']['numFound'] <= params['rows']+params['start']:
        print "Handled %s videos for wid %s" % (response['response']['numFound'], wid)
        break
    params['start'] += 100
