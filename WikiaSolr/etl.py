import json, requests, datetime, time, sys
from WikiaSolr import utils
from WikiaSolr.pageiditerator import PageIdIterator
from WikiaSolr.domain import sanitizeUrl

"""
Helps us push and pull data from app to Solr
"""
class SolrETL(object):

    def __init__(self, config, options):
        self.config, self.options = config, options
        self.useLogging = False
        self.useScribe = self.options.get('useScribe', False)
        self._configureHost()
        self._configureServices()
        self._configureDb()
        self.adds = []
        self.deletes = []
        self.proxies = [{'http':'varnish-s1'}] #{'http':'cache-s24'}
        self.defaultParams = { "controller": "WikiaSearchIndexer",
                                   "format": "json"
                             }

    def _configureHost(self):
        if self.useScribe:
            return
        self.hostsToIds = {}
        if self.options.get('file', False):
            try:
                events = json.loads("[%s]" % ",".join([line for line in open(self.options['file']).readlines() if line.strip() != ""]))
            except:
                sys.exit()
            for event in events:
                pageId = event.get("pageId", None)
                if pageId:
                    self.hostsToIds[event["serverName"]] = self.hostsToIds.get(event["serverName"], []) + [pageId]
        elif self.options.get('wikihost', False):
            iterator = PageIdIterator(self.options['wikihost'])
            self.hostsToIds[self.options['wikihost']] = [id for id in iterator] # todo: more scalable, less memory-intensive?
        elif self.options.get('wikiId', False):
            wikihost = utils.getWikiHostById(self.options['wikiId'])
            iterator = PageIdIterator(wikihost)
            self.hostsToIds[wikihost] = [id for id in iterator]
        else:
            raise Exception("We need a wiki host or file to run this script")

    def _configureServices(self):
        if self.options.get('serviceGroup', False):
            if self.options['serviceGroup'] in self.config["page"]["serviceGroups"]:
               self.services = self.config["page"]["serviceGroups"][self.options['serviceGroup']]
            else:
                raise Exception("The provided service group, %s, is not recognized. Please add it to the config if it is legitimate." % self.options['serviceGroup'])
        elif self.options.get('service', False):
            if self.options['service'] in self.config["page"]["services"] + self.config["wiki"]["services"]:
                self.services = [self.options['service']]
            else:
                raise Exception("The provided service, %s, is not recognized. Please add it to the config if it is legitimate." % self.options['service'])
        else:
            self.services = ["All"]

    def _configureDb(self):
        if self.useLogging or self.useScribe:
            import pymongo
            connection = pymongo.Connection(self.config["logging"]["host"], self.config["logging"]["port"])
            if self.useLogging:
                self.logEvents = connection.log.events
            if self.useScribe:
                self.scribeEvents = connection.scribe.events

    """
    Sends a request to our Solr end point given JSON data
    """
    def sendJsonToSolr(self, jsonData):
        endpoint = self.config["common"]["solr_endpoint"] + "update/?commit=true"
        request = requests.post(endpoint, \
                                data=json.dumps(jsonData), \
                                headers={u'Content-type':'application/json'})
        if request.status_code != 200:
            return self.logErrorFromRequest(jsonData, request, endpoint )
        contents = self.loadJsonFromRequest(request)
        if not contents:
            # we've got nothing
            return False
        if contents["responseHeader"]["status"] != 0:
            return self.logErrorFromResponse(jsonData, contents)
        return True

    """ Log an error from bad request """
    def logErrorFromRequest(self, jsonData, request, endpoint):
        if self.useLogging:
            self.logEvents.insert({
                               "data":jsonData,
                           "endpoint": endpoint,
                      "response_text": request.text,
                "request_status_code": request.status_code,
                          "timestamp": datetime.datetime.utcnow()
            })
        return False

    """ If the response says hey, there's an error """
    def logErrorFromResponse(self, jsonData, responseContents):
        print "ERROR: " + responseContents["error"]["msg"]
        if self.useLogging:
            self.logEvents.insert({
                                  "data":jsonData,
                "response_error_message": responseContents["error"]["msg"],
                  "response_status_code": responseContents["responseHeader"]["status"],
                             "timestamp": datetime.datetime.utcnow()
            })
        return False

    """ If a response is malformed from Solr"""
    def logBadResponse(self, response):
        if self.useLogging:
            self.logEvents.insert({
                "response_text": response.text,
                    "timestamp": datetime.datetime.utcnow(),
                   "json_error": 1
            })
        return False

    """Logs a successful update to Solr"""
    def logSuccess(self, host, service, ids):
        if self.useLogging:
            self.logEvents.insert({
                  "success": 1,
                "timestamp": datetime.datetime.utcnow(),
                     "host": host,
                  "service": service,
                      "ids": ids
            })
        return True

    """ Fault-tolerant JSON loading """
    def loadJsonFromRequest(self, requestResponse):
        try:
            return json.loads(requestResponse.text)
        except:
            return self.logBadResponse(requestResponse)

    """Accesses the application with the appropriate params and returns JSON"""
    def getJsonFromApplication(self, host, params):
        endpoint = sanitizeUrl(host)+'/wikia.php'
        try:
            request = requests.get(endpoint, params=params)#, proxies=random.choice(self.proxies))
            self.currentUrl = request.url
            if request.status_code != 200:
                return self.logErrorFromRequest(params, request, endpoint)
            return self.loadJsonFromRequest(request)
        except requests.exceptions.ConnectionError:
            print "Connection error for %s" % endpoint
        return {}
        

    def prepJson(self, json):
        self.adds += [doc for doc in json if u'id' in doc]
        self.deletes += [doc for doc in json if u'delete' in doc]
        return self.adds #@todo refactor other etl functions not to use this

    """ Calls basic application service API and sends response to Solr """
    def etlForPages(self):
        for host in self.hostsToIds.keys():
            self.currentLen = len(self.hostsToIds[host])
            print self.currentLen, "documents to index for", sanitizeUrl(host)
            for interval in range(0, len(self.hostsToIds[host]), 10):
                self.currentInterval = interval
                pageIds = self.hostsToIds[host][interval:interval+10]
                params = self.defaultParams.copy()
                for service in self.services:
                    params["ids"] = "|".join([str(pid) for pid in pageIds])
                    params["service"] = service
                    params["method"] = "get"
                    appJson = self.getJsonFromApplication(host, params)
                    if appJson:
                        self.prepJson(appJson['contents'])
                if len(self.adds) >= 250:
                    self.sendAddsAndDeletes()
        self.sendAddsAndDeletes()
        
    def sendAddsAndDeletes(self):
        if len(self.adds):
            print "Sending %d documents: %s ... (%d of %d)" \
                % (len(self.adds), ", ".join( [doc['id'] for doc in self.adds[:15]] ), min(self.currentInterval + 10, self.currentLen), self.currentLen)
            self.sendJsonToSolr(self.adds)
        if len(self.deletes):
            deleteJson = {'delete':[{'id': doc['delete']['id']} for doc in self.deletes if 'delete' in doc and 'id' in doc['delete']]}
            self.sendJsonToSolr(deleteJson)
        self.adds = []
        self.deletes = []

    def etlFromMongo(self):
        while True:
            update = {
                "$set": {"taken": datetime.datetime.utcnow(),
                          "available": 0,
                          "services": self.services}
            }
            self.hostsToIds = {}
            if self.options.get('wikihost', False):
                # if you have a host set, then you should continuously poll the host
                results = self.scribeEvents.find({"serverName":self.options['wikihost'], "available":1})
                self.scribeEvents.update({u'_id': {"$in": [result[u'_id'] for result in results]}}, update, multi=True)
                self.hostsToIds[self.options['wikihost']] = [result["pageId"] for result in results]
            else:
                for i in range(1, 4):
                    results = self.scribeEvents.find({"priority": i, "available":1})
                    resultLength = results.count()
                    if resultLength > 0:
                        results = list(results[:min(resultLength, 100)])
                        self.scribeEvents.update({u'_id': {"$in": [result[u'_id'] for result in results]}}, update, multi=True)
                        for result in results:
                            host, pageid = result[u'serverName'], result[u'pageId']
                            self.hostsToIds[host] = self.hostsToIds.get(host, []) + [pageid]
                        break
                # otherwise, grab the highest-priority host group; set it to taken
                pass
            self.etlForPages()


    """ Allows us to do multiple atromic updates with the same data across a wiki"""
    def etlForWiki(self):
        for service in self.services:
            if service not in self.config["wiki"]["services"]:
                raise Exception( "The service '%s' is not qualified to operate against a full wiki" % service )
            params = self.defaultParams.copy()
            params["service"] = service
            params["method"] = "getForWiki"
            serviceJson = self.getJsonFromApplication(self.options['wikihost'], params)
            if not serviceJson:
                continue
            wid = serviceJson[u'wid']
            iterator = PageIdIterator(self.options['wikihost'])
            updateJson = []
            counter = 0
            pageids = []
            for pageid in iterator:
                pageJson = serviceJson[u'contents'].copy()
                pageJson[u'id'] = u'%s_%d' % (wid, pageid)
                updateJson = updateJson + [pageJson]
                counter += 1
                pageids.append(pageid)
                if counter % 200 == 0 :
                    if self.sendJsonToSolr(updateJson):
                        self.logSuccess(self.options['wikihost'], service, pageids)
                    updateJson, pageids = [], []
            if self.sendJsonToSolr(updateJson):
                self.logSuccess(self.options['wikihost'], service, pageids)

