import sys, re, json, requests
from operator import itemgetter
from WikiaSolr import QueryIterator, get_config

wid = sys.argv[1]

config = json.loads(''.join(open('worker-config.json').readlines()))
host = config['common']['solr_endpoint']

qi = QueryIterator(get_config(), {'query': 'wid:%s AND suspected_entities_txt:*' % wid, 'fields': 'id,suspected_entities_txt,confirmed_entities_txt'})
#qi = QueryIterator(get_config(), {'query': 'wid:%s AND iscontent:true' % wid, 'fields': 'id,suspected_entities_txt,confirmed_entities_txt', 'filterquery': 'views:[2000000 TO *]'})

suspected_entities = {}
confirmed_entities = {}

def hasalpha(string):
    import string as s
    if string:
        for letter in s.lowercase:
            if letter in string:
                return True
    return False

def normalize(string):
    string = re.sub(u'[^\w\s]|_', u' ', string.lower())
    string = re.sub(u' {2,}', u' ', string)
    return string.strip()

count = 0

for doc in qi:
    for se in doc.get('suspected_entities_txt', []):
        se = normalize(se)
        suspected_entities[se] = suspected_entities.setdefault(se, 0) + 1
    for ce in doc.get('confirmed_entities_txt', []):
        ce = normalize(ce)
        confirmed_entities[ce] = confirmed_entities.setdefault(ce, 0) + 1
    count += 1

print 'COUNT: %i' % count

#suspected_entities_sorted = sorted(suspected_entities.items(), key=itemgetter(1), reverse=True)
#confirmed_entities_sorted = sorted(confirmed_entities.items(), key=itemgetter(1), reverse=True)
suspected_entities_sorted = sorted([(entity, count) for (entity, count) in suspected_entities.items() if hasalpha(entity)], key=itemgetter(1), reverse=True)
confirmed_entities_sorted = sorted([(entity, count) for (entity, count) in confirmed_entities.items() if hasalpha(entity)], key=itemgetter(1), reverse=True)
unconfirmed_entities_sorted = sorted([(entity, count) for (entity, count) in suspected_entities.items() if entity not in confirmed_entities and hasalpha(entity)], key=itemgetter(1), reverse=True)

print '\nSUSPECTED ENTITIES:'
for entity, count in suspected_entities_sorted[:50]:
    print '%i\t%s' % (count, entity)

print '\nCONFIRMED ENTITIES:'
for entity, count in confirmed_entities_sorted[:50]:
    print '%i\t%s' % (count, entity)

print '\nUNCONFIRMED ENTITIES:'
for entity, count in unconfirmed_entities_sorted[:50]:
    print '%i\t%s' % (count, entity)
