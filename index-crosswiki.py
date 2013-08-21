from wikicities import LoadBalancer
import requests, json, os

print 'mapping wiki ids to urls...'
dbyml = os.environ.get('WIKIA_DB_YML', '/usr/wikia/conf/current/DB.yml')
lb = LoadBalancer(dbyml)
globalDb = lb.get_db_by_name('wikicities')
globalCursor = globalDb.cursor()
query = 'SELECT city_id, city_url FROM city_list WHERE city_public = 1' # natural sort is primary here
count = globalCursor.execute(query)

for i in range(0, count):
    (id, url) = globalCursor.fetchone()
    print url,
    try:
        r = requests.get(url+'/wikia.php', \
                                     params={'controller':'WikiaSearchIndexer', 
                                             'method':'getForWiki', 
                                             'service':'CrossWikiCore', 
                                             'format':'json'})
        if r.status_code != 200:
            print "error"
            continue
        goods = json.loads(r.content)
        update = [goods['contents']]
        update[0]['id'] = goods['wid']
        r = requests.post('http://dev-search.prod.wikia.net:8983/solr/xwiki/update?commit=true', \
                              data=json.dumps(update), \
                              headers={u'Content-type':'application/json'})
        print url, r.content
    except ValueError as e:
        print "there was an error", e
        pass
        #fuck it
