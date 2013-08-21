import tldextract, logging, os
from urlparse import urljoin, urlparse, urlunparse
from wikicities import LoadBalancer

logging.basicConfig()

PRE_PRODUCTION_ENVS = ['preview', 'verify', 'sandbox-s1', 'sandbox-s2', 'sandbox-s3', 'sandbox-s4']
prodUrls = {}
dbnames = {}

""" 
Transforms a URL to what we believe to be its production URL based on the one provided to us.
Attempts to strip pre-production subdomains and dev-box info from a URL with no knowledge of the database.
"""
def sanitizeUrl(url):
    parsed = urlparse(url)
    extracted = tldextract.extract(parsed.netloc.lower())
    newextracted = list(extracted)
    if extracted.domain == 'wikia-dev':
        splitsubdomains = extracted.subdomain.split('.')[:-1]
        newextracted[1] = 'wikia'
    else:
        splitsubdomains = extracted.subdomain.split('.')
    newextracted[0] = '.'.join(filter(lambda a: a not in PRE_PRODUCTION_ENVS, splitsubdomains))
    netloc = '.'.join(newextracted)
    reparse = list(tuple(parsed))
    reparse[1] = netloc
    return urlunparse(reparse)


class WikiaDomainLoader(object):
    def __init__(self, subOrUrl, env='prod', user=None):
        self.subOrUrl = subOrUrl
        self.ownDomain = False
        self.wikia = False
        self.tld = 'com'
        self.path = ''
        self._configureSubdomain()
        self.domainInstance = self._getDomainInstance(env, user)

    def _configureSubdomain(self):
        """ Sets the subdomain string, given a subdomain or a URL """
        subOrUrl = self.subOrUrl
        if '.' in subOrUrl:
            if '//' not in subOrUrl:
                subOrUrl = 'http://' + subOrUrl
            url = urlparse(subOrUrl)
            netloc = tldextract.extract(url.netloc.lower())
            self.path = url.path
            sublist = netloc.subdomain.split('.')
            sublistCopy = sublist[:]
            if netloc.domain == 'wikia-dev':
                if len(sublist) > 2:
                    for item in sublist[:-2]:
                        if len(item) != 2:
                            sublistCopy.remove(item)
                self.sub = '.'.join(sublistCopy[:-1])
            elif netloc.domain == 'wikia':
                self.wikia = True
                self.sub = '.'.join(sublistCopy)
            else:
                self.sub = '.'.join([netloc.subdomain, netloc.domain]) if netloc.subdomain != 'www' else netloc.domain
                self.tld = netloc.tld
                self.ownDomain = True
        else:
            self.sub = subOrUrl
        self._removePreproductionSubdomains()

    """ Removes environments we don't want """
    def _removePreproductionSubdomains(self):
        global PRE_PRODUCTION_ENVS
        self.sub = '.'.join(filter(lambda a: a not in PRE_PRODUCTION_ENVS, self.sub.split('.')))

    def _getDomainInstance(self, env, user):
        """ Returns appropriate Domain object, instantiating if nonexistent """
        if not hasattr(self, 'domainInstance'):
            if env == 'prod':
                self.domainInstance = ProdDomain(self.sub, own=self.ownDomain, wikia=self.wikia, tld=self.tld)
            elif env == 'dev':
                self.domainInstance = DevDomain(self.sub, user=user, own=self.ownDomain, wikia=self.wikia, tld=self.tld)
            else:
                self.domainInstance = StagingDomain(self.sub, env=env, own=self.ownDomain, wikia=self.wikia, tld=self.tld)
        return self.domainInstance

    def getUrl(self, path=None):
        """ Returns URL string, given optional path argument """
        if not path:
            path = self.path
        return urljoin(self.domainInstance.getDomain(), path)

class ProdDomain(object):
    def __init__(self, sub, env=None, user=None, own=False, wikia=False, tld='com'):
        self.sub = sub
        self.env = env
        self.user = user
        self.own = own
        self.tld = tld
        self.dbname = None
        self.wikia = wikia
        self.domain = self.configureDomain()

    def getDomain(self):
        """ Accessor for domain attribute """
        return self.domain

    def setSub(self, sub):
        """ Mutator for subdomain attribute """
        self.sub = sub

    def getSub(self):
        """Accessor for subdomain attribute"""
        return self.sub

    def configureDomain(self):
        """ Configures the domain based on sub, env, and user parameters """
        raise NotImplementedError
    
    def configureDomain(self):
        """ Figures out the domain and dbname for a given URL """
        global prodUrls
        if not (self._getFromMemory() or self._getFromDb()):
            return self.prodUrl
            if self.own and not self.sub.count('.'):
                subPlusDomain = 'www.%s' % (self.sub)
            else:
                subPlusDomain = '%s.wikia' % self.sub
            self.prodUrl = 'http://%s.%s' % (subPlusDomain, self.tld)
        prodUrls[self.sub] = self.prodUrl
        return self.prodUrl

    def getDbName(self):
        return self.dbname

    """ Verifies a production URL using the database, stores that value and the dbname in memory """
    def _getFromDb(self):
        global dbnames
        dbyml = os.environ.get('WIKIA_DB_YML', '/usr/wikia/conf/current/DB.yml')
        lb = LoadBalancer(dbyml)
        citiesDb = lb.get_db_by_name('wikicities')
        cursor = citiesDb.cursor()
        shoulddub = 'www.' if not self.sub.count('.') else ''
        ownUrl = "http://%s%s.%s/" % (shoulddub, self.sub, self.tld)
        wikiaUrl = "http://%s.wikia.com/" % self.sub
        if self.own:
            query = """
SELECT city_url, city_dbname
FROM city_list 
WHERE city_url="%s"
LIMIT 1
""" % (ownUrl)
        elif self.wikia:
            query = """
SELECT city_url, city_dbname
FROM city_list 
WHERE city_url="%s"
LIMIT 1
""" % (wikiaUrl)
        else:
            query = """
SELECT city_url, city_dbname, CASE WHEN city_url="%s" THEN 2 WHEN city_url="%s" THEN 1 ELSE 0 END is_url
FROM city_list 
WHERE (city_url = "%s" OR city_url = "%s" OR city_sitename= "%s" OR city_dbname="%s")
ORDER BY is_url DESC
LIMIT 1
""" % (ownUrl, wikiaUrl, ownUrl, wikiaUrl, self.sub, self.sub)
        numFound = cursor.execute(query)
        if numFound:
            result = cursor.fetchone()
            (self.prodUrl, dbnames[self.sub]) = result[:2]
            return True
        else:
            return False

    """ In-memory storage, bruh """
    def _getFromMemory(self):
        global prodUrls
        self.prodUrl = prodUrls.get(self.sub, False)
        self.dbname = dbnames.get(self.sub, False)
        return self.prodUrl and self.dbname

class DevDomain(ProdDomain):
    """ A dev domain uses the dbname and the provided user prefix """
    def getDomain(self):
        sub = self.sub if not self.getDbName() else self.getDbName()
        return 'http://%s.%s.wikia-dev.com' % (sub, self.user)

class StagingDomain(ProdDomain):
    """ A 'staging' domain is like a prod domain with an environment prefix domain before any number of subdomains """
    def getDomain(self):
        ex = tldextract.extract(self.prodUrl)
        subs = '.'.join(ex.subdomain) if not isinstance(ex.subdomain, str) else ex.subdomain
        return "http://%s.%s.%s.%s/" % (self.env, subs, ex.domain, ex.tld)
        

if __name__ == '__main__':
    urls = [
    'http://muppet.wikia.com',
    'http://muppet.wikia.com/wiki/Kermit_the_Frog',
    'muppet',
    'http://ru.avatar.wikia.com',
    'http://www.ru.avatar.robert.wikia-dev.com/wiki/',
    'http://sandbox-s2.ru.avatar.wikia.com/',
    'preview.muppet.wikia.com',
    'http://www.wowwiki.com',
    'http://www.wowwiki.robert.wikia-dev.com/wiki/Portal:Main',
    'http://preview.www.wowwiki.com/Portal:Main',
    'www.wikia.com',
    'preview.www.wikia.com',
    'www.robert.wikia-dev.com',
    'http://en.memory-alpha.org/wiki/Portal:Main',
    'http://pt-br.pokepediabr.wikia.com'
    ]

    for url in urls:
        print url
        print WikiaDomainLoader(url, env='prod').getUrl()
        print WikiaDomainLoader(url, env='dev', user='tristan').getUrl()
        print WikiaDomainLoader(url, env='preview').getUrl('foo.php')
        print ''
