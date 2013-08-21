import requests, json, os, sys, random

try:
    r = requests.get(sys.argv[1]+'/wikia.php', \
                                     params={'controller':'WikiaSearchIndexer', 
                                             'method':'getForWiki', 
                                             'service':'CrossWikiCore', 
                                             'format':'json'})
    if r.status_code != 200:
        print sys.argv[1], r.content
        sys.exit()
    goods = json.loads(r.content)
    update = [goods['contents']]
    update[0]['id'] = goods['wid']
    r = requests.post('http://search-s11.prod.wikia.net:8983/solr/xwiki/update', \
                          data=json.dumps(update), \
                          headers={u'Content-type':'application/json'})
    print sys.argv[1], r.content
except ValueError as e:
    print "there was an error", e
    pass

