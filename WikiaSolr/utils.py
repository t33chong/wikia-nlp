from wikicities import LoadBalancer

"""
Utility functions
"""

def getWikiHostById(id):
    lb = LoadBalancer('/usr/wikia/conf/current/DB.yml')
    db = lb.get_db_by_name('wikicities')
    cursor = db.cursor()
    result = cursor.execute("SELECT city_url FROM city_list WHERE city_id = %d LIMIT 1" % int(id))
    if result:
        return cursor.fetchone()[0]
