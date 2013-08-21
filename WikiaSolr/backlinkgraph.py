"""
WikiaSolr Module
Allows us to 'graph' backlinks, provided a set of docs.
"""
from WikiaSolr.queryiterator import QueryIterator
from multiprocessing import Pool

""" Return a list of tuples of docId, list of backlinks """ 
def mapBacklinks(resultSet):
    results = []
    for doc in resultSet:
        for link in doc["outbound_links_txt"]:
            ploded = link.split(" | ")
            docId, text = ploded[0], " | ".join(ploded[1:])
            results.append((docId, text))
    return results

""" Brings all our backlinks together """
def reduceBacklinks(backlinkMapping):
    return (backlinkMapping[0], [mapping[1] for mapping in backlinkMapping[1]])

""" Dictifies our tuples so we can run our reduce operation """
def partitionBacklinks(L):
    docIdsToMappings = {}
    for sublist in L:
        for p in sublist:
            try:
                docIdsToMappings[p[0]].append(p)
            except KeyError:
                docIdsToMappings[p[0]] = [p]
    return docIdsToMappings


class BacklinkGraph(object):
    def __init__(self, config, query, threads):
        self.query = query
        self.fields = 'id,outbound_links_txt'
        self.rows = 200
        self.threads = int(threads)
        """ Key is doc ID, value is a list of outbound links """

        #print "Generating iterators..."
        iterator = QueryIterator(config,{'query':self.query, 'rows':self.rows, 'fields':'id,pageid,outbound_links_txt'})
        iterators = [iterator]
        for i in range(1, self.threads):
            self.start = int((float(iterator.numFound)/float(self.threads)) * i)
            limit = int((float(iterator.numFound)/float(self.threads)) * (i+1))
            iterators.append(QueryIterator(config,{'query':self.query, 'rows':self.rows, 'start':self.start, 'limit':limit, 'fields':'id,pageid,outbound_links_txt'}))

        pool = Pool(processes=self.threads)

        #print "Mapping..."
        full_tuples = pool.map(mapBacklinks, iterators)

        #print "Partitioning..."
        id_to_tuples = partitionBacklinks(full_tuples)

        #print "Reducing..."
        self.backlinks = pool.map(reduceBacklinks, id_to_tuples.items())

        #print "Backlinks resolved"

    """
    We could add some methods for handling graph analysis here at some point.
    That point isn't today.
    """
