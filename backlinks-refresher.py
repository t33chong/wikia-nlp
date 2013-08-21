"""
Provided a query, wiki ID, or host name, iterates over all
results in Solr and then iterates over all documents storing 
that result as an outbound link, storing those values in the 
backlinks_txt field.
"""
import requests
from optparse import OptionParser
import json

parser = OptionParser()
parser.add_option("-i", "--wikiid", dest="wiki", action="store", default=None,
                  help="Specifies the wiki ID to perform this task against")
parser.add_option("-q", "--query", dest="query", action="store", default=None,
                  help="Specifies a query to perform this task against")
parser.add_option("-w", "--wikihost", dest="host", action="store", default=None,
                  help="Specifies a wikihost to perform this calculation against")

(options, args) = parser.parse_args()

config = json.loads("".join(open('worker-config.json').readlines()))

if options.wiki:
    query = "wid:%s" % options.wiki
elif options.host:
    query = "host:%s" % options.host.replace("http://", "")
elif options.query:
    query = options.query
else:
    raise Exception("Please provide either a wiki, query, or host")

"""
Iterate over all docs with outbound links matching the id of the present document.
"""
def get_backlinks(endpoint_url, doc):
    backlinks = []
    try:
        data = {
             'q': 'outbound_links_txt:"%s |"' % (doc['id']),
            'fl': ['id', 'outbound_links_txt'],
          'rows': 100,
        'offset': 0,
            'wt': 'json'
        }
        while True:
            try:
                response = requests.get(endpoint_url+"select", params=data).content
                backlink_response = json.loads(response)
                for backlink_doc in backlink_response['response']['docs']:
                    backlinks += [' | '.join(link.split(' | ')[1:]) for link in backlink_doc['outbound_links_txt'] if link.startswith('%s |' %doc['id'])]
            except:
                pass
            if backlink_response['response']['numFound'] <= data['rows'] + data['offset']:
                break
            data['offset'] += 100
        if len(backlinks) > 0:
            doc['backlinks_txt'] = {'set':backlinks}
    except:
        pass
    return doc

endpoint = config["common"]["solr_endpoint"]
data = {'q':query, 'fl':'id', 'rows':100, 'offset':0, 'wt':'json'}
while True:
    try:
        response = json.loads(requests.get(endpoint+"select", params=data).content)
        backlink_updates = [get_backlinks(endpoint, doc) for doc in response['response']['docs']]
        json_data = json.dumps([doc for doc in backlink_updates if doc.get('backlinks_txt', None) is not None])
        requests.post(endpoint+"update?commit=true", data=json_data, headers={u'Content-type':'application/json'})
    except:
        pass
    if data['rows'] + data['offset'] >= response['response']['numFound']:
        break
    data['offset'] += 100

