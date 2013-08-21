import phpserialize, sys, jsonrpclib, random
from wikicities import LoadBalancer
from WikiaSolr import DismaxQueryIterator, get_config, get_entities, as_string, expand_entities

SEARCH_TOPICS_ID = 1310 # this is really just a note

nlp_server = jsonrpclib.Server('http://localhost:8080')

config = get_config()

lb = LoadBalancer('/usr/wikia/conf/current/DB.yml')
cities = lb.get_db_by_name('wikicities')
cursor = cities.cursor()

db_results = cursor.execute( \
    """
SELECT city_list.city_id, city_list.city_dbname, city_list.city_url, al.cv_value
FROM
  ( SELECT cv_value, cv_city_id FROM city_variables WHERE cv_variable_id = %s ) al
INNER JOIN city_list
ON al.cv_city_id = city_list.city_id;
    """ % SEARCH_TOPICS_ID )

requestedFields = 'id,html_en,title_en,video_actors_txt,video_genres_txt,video_keywords_txt,video_description_txt,video_tags_txt'
queryFields = 'html_en,title_en,nolang_txt,video_actors_txt,html_media_extras_txt'

results = [ i for i in cursor.fetchall() ]
random.shuffle(results)

for result in results:
    (wiki_id, wiki_dbname, wiki_url, topic_result) = result
    if wiki_url == 'http://en.memory-alpha.org/':
        continue #why ain't this workin
    print wiki_url
    topics = phpserialize.dict_to_list(phpserialize.loads(topic_result))
    query = "wid:298117 AND -title_en:\"File:IGN Weekly 'Wood\" is_video:true AND ( %s )' " % " OR ".join(['"%s"' % topic for topic in topics])

    queryIterator = DismaxQueryIterator(config, {'query':query, 'fields':requestedFields, 'queryFields':queryFields })
    entities = {}
    confirmed_entities = {}
    print "Working over %d docs..." % queryIterator.numFound
    for doc in queryIterator:
        print '================= %s ================' % doc['id']
        fields = []
        for field in ['title_en', 'video_description_txt', 'video_keywords_txt', 'video_actors_txt', 'video_tags_txt', 'video_genres_txt' ]:
            val = doc.get(field, None)
            if val:
                fields.append(as_string(doc[field]))
        text = ".\n".join(fields)
        print text
        doc_entities = get_entities(nlp_server, text)
        expanded_entities = dict([ item for item in expand_entities(doc_entities, wiki_url).items() if item[1] != ''])
        entities[doc['id']] = doc_entities + expanded_entities.values()
        confirmed_entities[doc['id']] = expanded_entities.keys() + expanded_entities.values()
        print 'suspected', entities[doc['id']]
        print 'confirmed', confirmed_entities[doc['id']]
        print '======================================='
    print "Entities: ", entities.values()
    print "Confirmed Entities: ", confirmed_entities.values()
    # send update here
    sys.exit()



