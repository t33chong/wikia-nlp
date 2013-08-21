from wikicities import LoadBalancer
from WikiaSolr.freebase import *
from WikiaSolr.entities import *
import jsonrpclib

def get_entities_from_description(server):
    lb = LoadBalancer('/usr/wikia/conf/current/DB.yml')
    cities = lb.get_db_by_name('wikicities')
    cursor = cities.cursor()
    sql = \
        """
        SELECT city_id, city_dbname, city_url, city_description
        FROM city_list
        WHERE city_description IS NOT NULL
          AND city_description != ''
          AND city_description NOT LIKE '%knowledge!' -- boilerplate
          AND city_lang LIKE 'en%'
        """
    results = cursor.execute(sql)
    print "Working over %d results" % results
    for (city_id, city_dbname, city_url, city_description) in cursor.fetchall():
        print "=" * 15
        print city_url
        suspected = get_entities(server, city_description)
        expanded = expand_entities(suspected, city_url)
        confirmed = set([key for key in expanded.keys() if expanded[key]] + [value for value in expanded.values() if value ])
        freebased_tuples = [freebase_entity(entity) for entity in confirmed]
        freebased = dict([fb for fb in freebased_tuples if fb[1]])
        print "suspected: %s" % ", ".join( suspected )
        print "confirmed: %s" % ", ".join( confirmed )
        print "freebase data for %s" % ", ".join( freebased.keys() )
        print "=" * 15

get_entities_from_description(jsonrpclib.Server('http://localhost:8080'))
