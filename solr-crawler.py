import os, json, shutil, time
from wikicities import LoadBalancer
from datetime import date
from optparse import OptionParser

parser = OptionParser()
parser.add_option("-m", "--minimum-wid", dest="minimumWid", action="store", default=None,
                  help="Allows us to start off at the last wiki ID processed")
parser.add_option("-c", "--counter", dest="counter", action="store", default=0,
                  help="Allows us to use a different value than 0 for counter")
parser.add_option("-f", "--file-size", dest="filesize", action="store", default=50000,
                  help="Allows us to use a different value than 0 for counter")
parser.add_option("-d", "--scribe-dir", dest="scribedir", action="store", default='/var/spool/scribe',
                  help="Allows us to use a different value than 0 for counter")
(options, args) = parser.parse_args()

started = time.time()

print 'mapping wiki ids to urls...'
dbyml = os.environ.get('WIKIA_DB_YML', '/usr/wikia/conf/current/DB.yml')
lb = LoadBalancer(dbyml)
globalDb = lb.get_db_by_name('wikicities')
globalCursor = globalDb.cursor()
query = 'SELECT city_id, city_url FROM city_list' # natural sort is primary here
if options.minimumWid:
    query += ' WHERE city_id > %d ORDER BY city_id ASC' % int(options.minimumWid) # oh haha very funny, sort changes
globalCursor.execute(query)

# store hashmap of ids to URLs in memory
idsToUrls = dict(globalCursor)

# memory management
globalCursor.close()
del globalCursor, globalDb

ts = date.today().isoformat()
counter = int(options.counter)
def writeEvents(events):
    global ts, counter, options
    for i in range(0, len(events), options.filesize):
        fname = 'bulk-%s_%s' % (ts, str(counter).zfill(5))
        eventslice = events[i:i+options.filesize]
        print "Writing %d events to %s" % (len(eventslice), fname)
        f = open('/tmp/%s' % (fname), 'w')
        f.write("\n".join([json.dumps(e) for e in eventslice]))
        f.close()
        shutil.move('/tmp/%s' % (fname), '%s/bulk/%s' % (options.scribedir,fname))
        counter += 1

events = []
wids = idsToUrls.keys()
wids.sort()
for wikiId in wids:
    url = idsToUrls[wikiId]
    lb = LoadBalancer(dbyml) # to prevent mysql server from going away
    dataware = lb.get_db_by_name('dataware')
    print "Getting pages for wid %d" % wikiId
    pageCursor = dataware.cursor()
    pageCursor.execute('SELECT page_id FROM pages WHERE page_wikia_id = %d' % wikiId)
    eventCounter = 0
    events += [{'serverName':url, 'pageId':pageId} for (pageId) in pageCursor]
    if len(events) > options.filesize:
        writeEvents(events)
        events = []

print "Finished in %d seconds" % (time.time() - started)
