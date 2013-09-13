from WikiaSolr.queryiterator import QueryIterator
from WikiaSolr.groupedqueryiterator import GroupedQueryIterator
from wikicities.DB import LoadBalancer
from corenlp.corenlp import *
from nlp_rest_client import SolrWikiService
from subprocess import Popen, PIPE
from datetime import datetime
import json, requests, os, time
"""
Responsible for threading workers over grouped queries
"""
class Overseer(object):
    """ Preconfiguration. Some of this is because we are reusing the optparse options too much. """
    def __init__(self, config, options = {}):
        self.setOptions(options)
        self.config = config
        self.options = options
        self.processes = {}
        self.timings = {}

    """ Configures options from dict """
    def setOptions(self, options={}):
        options['groupField'] = 'wid'
        options['query'] = self.getQuery() # ensures the wiki is worth dealing with
        if options['wam_threshold']:
            options['query'] += " AND wam:[%s TO 100]" % options['wam_threshold']
        options['fields'] = 'wid'
        options['groupRows'] = None
        options['verbose'] = True
        self.options = options


    """ 'Abstract' method for specifying what kinds of wikis to search for when grouping """
    def getQuery(self):
        raise NotImplementedError("This should be implemented by each child class")

    """ 'Abstract' method for firing off a child process """
    def add_process(self, group):
        raise NotImplementedError("This should be implemented by each child class")

    """ By default we use a GroupedQueryIterator """
    def getIterator(self):
        return GroupedQueryIterator(self.config, self.options)

    """ This is set to run indefinitely """
    def oversee(self):
        while True:
            options = {}
            iterator = self.getIterator()
            for group in iterator:
                while len(self.processes.keys()) == int(self.options['workers']):
                    time.sleep(5)
                    self.check_processes()
                self.add_process(group)

    """ If a process is finished, report it and drop it """
    def check_processes(self):
         for pkey in self.processes.keys():
             if self.processes[pkey].poll() is not None:
                 if self.options.get('verbose', False):
                     print "Finished wid %s in %d seconds with return status %s" % (pkey, (datetime.now() - self.timings[pkey]).seconds, self.processes[pkey].returncode)
                 del self.processes[pkey], self.timings[pkey]

class NLPOverseer(Overseer):

    def setOptions(self, options={}):
        #if not options.get('modulo'):
        #    raise Exception('Must specify modulo 0 or 1.')
        self.options = options
        options['query'] = self.getQuery() # ensures the wiki is worth dealing with
        options['fields'] = 'id'
        options['verbose'] = True

    def getIterator(self):
        return QueryIterator('http://dev-search.prod.wikia.net:8983/solr/xwiki/', self.options)

    def getQuery(self):
        return 'lang_s:%s' % self.options.get('language', 'en')

    def add_process(self, group):
        wid = group["id"]
        print "Starting process for wid %s" % wid
        command = 'python %s %s %s %s' % (os.path.join(os.getcwd(), 'nlp-harvester.py'), str(wid), str(self.options['language']), str(self.options['last_indexed']))
        process = Popen(command, shell=True)
        self.processes[wid] = process
        self.timings[wid] = datetime.now()

    def oversee(self):
        while True:
            options = {}
            iterator = self.getIterator()
            skip = [298117]
            #if os.path.exists('/data/xml'):
            #    skip.extend(os.listdir('/data/xml'))
            for group in iterator:
                #if int(group['id']) % 2 == int(self.options['modulo']):

                # SKIP VIDEO WIKI + "DONE"
                if group['id'] in skip:
                    print 'skipping %s...' % group['id']
                    continue
                while len(self.processes.keys()) == int(self.options['workers']):
                    time.sleep(1)
                    self.check_processes()
                self.add_process(group)

class EntityOverseer(Overseer):
    def __init__(self, options={}):
        super(EntityOverseer, self).__init__(options)
        self.setOptions(options)

    def setOptions(self, options={}):
        self.options = options
        self.csv = open(options.get('csv_file', '/data/wikis_to_entities.csv'), 'w')
        self.xml_dir = options.get('xml_dir', '/data/xml')

    def getIterator(self):
        return [int(wid) for wid in os.listdir(self.xml_dir)]

    def check_if_parsed(self, wid):
        response = SolrWikiService().get(wid)[wid]
        articles_i = response['articles_i']
        #url = response['url']
        wid_dir = os.path.join(self.xml_dir, str(wid))
        parsed_count = 0
        for (dirpath, dirnames, filenames) in os.walk(wid_dir):
            for filename in filenames:
                parsed_count += 1
        percent_complete = float(parsed_count)/articles_i
        sys.stdout.write('wid %i is %i%s complete, ' % (wid, percent_complete*100, '%'))
        if percent_complete >= 0.9:
            print 'harvesting named entities...'
            return True
        print 'skipping...'
        return False

    def add_process(self, wid):
        if self.check_if_parsed(wid):
            print "Starting process for wid %i" % wid
            command = 'python %s %i' % (os.path.join(os.getcwd(), 'entity-harvester.py'), wid)
            #command = 'python %s %i' % (os.path.join(os.getcwd(), 'foo.py'), wid)
            process = Popen(command, stdout=PIPE, shell=True)
            self.processes[wid] = process
            self.timings[wid] = datetime.now()

    def oversee(self):
        try:
            while True:
                options = {}
                iterator = self.getIterator()
                for group in iterator:
                    while len(self.processes.keys()) == int(self.options['workers']):
                        #time.sleep(1)
                        self.check_processes()
                    self.add_process(group)
        # make sure file is closed when manually stopping the overseer
        except KeyboardInterrupt:
            self.csv.close()

    def check_processes(self):
         for pkey in self.processes.keys():
             if self.processes[pkey].poll() is not None:
                 if self.options.get('verbose', False):
                     print "Finished wid %s in %d seconds with return status %s" % (pkey, (datetime.now() - self.timings[pkey]).seconds, self.processes[pkey].returncode)
                 self.csv.write(self.processes[pkey].stdout.read())
                 del self.processes[pkey], self.timings[pkey]

"""
Oversees the administration of backlink processes
"""
class BacklinkOverseer(Overseer):
    """ Selects only wikis with outbound links """
    def getQuery(self):
        return 'outbound_links_txt:*'

    """ Fires off a backlink harvester """
    def add_process(self, group):
        wid = group["groupValue"]
        print "Starting process for wid %s" % wid
        args = ["python", os.getcwd() + "/backlink-harvester.py", str(wid), str(self.options['threads'])]
        process = Popen(args)
        self.processes[wid] = process
        self.timings[wid] = datetime.now()

class KillBacklinksOverseer(Overseer):
    def getQuery(self):
        return 'id:*'

    def add_process(self, group):
        wid = group["groupValue"]
        print "Starting process for wid %s" % wid
        args = ["python", os.getcwd() + "/killbacklinks-harvester.py", str(wid), str(self.options['threads'])]
        process = Popen(args)
        self.processes[wid] = process
        self.timings[wid] = datetime.now()

"""
Overseer for NER storage
"""
class NamedEntityOverseer(Overseer):
    """ Selects English content wikis """
    def getQuery(self):
        return 'lang:en AND is_content:true'

    """ Fires off a video view harvester """
    def add_process(self, group):
        wid = group["groupValue"]
        print "Starting process for wid %s" % wid
        args = ["python", os.getcwd() + "/named-entity-harvester.py", str(wid), str(self.options['threads'])]
        process = Popen(args)
        self.processes[wid] = process
        self.timings[wid] = datetime.now()


"""
Oversees the administration of video view processes
"""
class VideoViewsOverseer(Overseer):
    """ Selects only wikis with videos """
    def getQuery(self):
        return 'is_video:true'

    """ Fires off a video view harvester """
    def add_process(self, group):
        wid = group["groupValue"]
        print "Starting process for wid %s" % wid
        args = ["python", os.getcwd() + "/video-views-harvester.py", str(wid), str(self.options['threads'])]
        process = Popen(args)
        self.processes[wid] = process
        self.timings[wid] = datetime.now()

"""
Iterates over all open wikis and writes scribe events
"""
class ScribeOverseer(Overseer):
    """ This is a MySQL query -- TODO: split out parent classes of Solr oversee and MySQL overseer """
    def getQuery(self):
        return 'SELECT `city_url` FROM `city_list` WHERE `city_public` = 1;'

    """ Here we are giving ourselves a MySQL cursor instead of one of our hand-rolled iterators """
    def getIterator(self):
        lb = LoadBalancer(self.options.get('dbconf', '/usr/wikia/conf/current/DB.yml'))
        globalDb = lb.get_db_by_name('wikicities')
        cursor = globalDb.cursor()
        self.results = cursor.execute(self.getQuery())
        self.progress = 0
        return cursor

    """ Fires off a scribe-writer process. 'group' param is a tuple returned from cursor """
    def add_process(self, group):
        url = group[0]

        print "(%s/%s) Starting process for %s" % (self.progress, self.results, url)
        args = ["python", os.getcwd() + "/scribe-writer.py", '--wikihost=%s' % url]
        process = Popen(args)
        self.processes[url] = process
        self.timings[url] = datetime.now()
        self.progress += 1

    """ Don't need the option prep. This should actually be the parent class's behavior when refactored """
    def setOptions(self, options={}):
        self.options = options

class ReindexOverseer(Overseer):

    def getQuery(self):
        return None

    def getIterator(self):
        self.progress = 0
        lines = [line[:-1].split(',')[0] for line in open(self.options['file']).readlines()]
        self.results = len(lines)
        return lines

    """ Calls a page-worker process on the current wiki ID """
    def add_process(self, group):
        print "Reindexing wid %s" % group
        args = ["python", os.getcwd() + "/page-worker.py", '--wiki-id=%s' % group]
        process = Popen(args)
        self.processes[group] = process
        self.timings[group] = datetime.now()
        self.progress += 1

    """ Don't need the option prep. This should actually be the parent class's behavior when refactored """
    def setOptions(self, options={}):
        self.options = options

"""
Allows us to index a wiki on our xwiki core
"""
class CrossWikiReindexOverseer(Overseer):
    """ This is a MySQL query -- TODO: split out parent classes of Solr oversee and MySQL overseer """
    def getQuery(self):
        query = 'SELECT `city_url` FROM `city_list` WHERE `city_public` = 1'
        if self.options.get('startfrom', False):
            query += 'AND `city_id` > %d' % int(self.options['offset'])
        return query

    """ Here we are giving ourselves a MySQL cursor instead of one of our hand-rolled iterators """
    def getIterator(self):
        lb = LoadBalancer(self.options.get('dbconf', '/usr/wikia/conf/current/DB.yml'))
        globalDb = lb.get_db_by_name('wikicities')
        cursor = globalDb.cursor()
        self.results = cursor.execute(self.getQuery())
        self.progress = 0
        return cursor

    """ Fires off a scribe-writer process. 'group' param is a tuple returned from cursor """
    def add_process(self, group):
        url = group[0]

        print "(%s/%s) Starting process for %s" % (self.progress, self.results, url)
        args = ["python", os.getcwd() + "/crosswiki-reindex-harvester.py", url]
        process = Popen(args)
        self.processes[url] = process
        self.timings[url] = datetime.now()
        self.progress += 1

    """ Don't need the option prep. This should actually be the parent class's behavior when refactored """
    def setOptions(self, options={}):
        self.options = options
