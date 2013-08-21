import WikiaSolr, pymongo, requests, json, sys, random

conn = pymongo.Connection()
db = conn.video

class Struct:
    def __init__(self, **entries): 
        self.__dict__.update(entries)

config = json.loads("".join(open('worker-config.json').readlines()))

start = sys.argv[1] if len(sys.argv) > 1 else 0

iterator = WikiaSolr.QueryIterator(config, Struct(**{ 'query': 'is_video:true AND wid:%s' % WikiaSolr.VIDEO_WIKI_ID, 'fields': 'pageid', 'rows':200, 'start':start}) )

docs = []
for doc in iterator:
    docs += [doc]
    if len(docs) % 10 == 0 or iterator.at == (iterator.numFound - iterator.firstStart):
        print '%s of %s...' % (iterator.at, iterator.numFound - iterator.firstStart)
        random.shuffle(docs) # avoid cache
        try:
            r = requests.get( 'http://sandbox-s2.video.wikia.com/wikia.php', \
                                  params={ 'controller' : 'WikiaSearchIndexer',
                                           'method'     : 'get',
                                           'service'    : 'MediaData',
                                           'ids'        : '|'.join([str(d['pageid']) for d in docs]),
                                           'cached'     : 'false'
                                           } )
            j = json.loads(r.content)
            print r.content
            db.video_metadata.insert(j['contents'])
            docs = []
        except:
            docs = []
            print r.content, sys.exc_info()

