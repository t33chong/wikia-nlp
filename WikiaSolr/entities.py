import requests, json, nltk, sys, jsonrpclib

def as_string(string_or_list):
    return ".\n".join(string_or_list) if type(string_or_list) is list else string_or_list

def get_entities(nlp_server, text):
    try:
        result = json.loads(nlp_server.parse(text))
    except (ValueError, jsonrpclib.jsonrpc.ProtocolError):
        print "Error parsing:", sys.exc_info()
        return []
    entities = []
    for sentence in result.get('sentences', []):
        # actually use NRE
        entityWords = []
        type = None
        lastType = None
        for [word, data] in sentence.get('words', []):
            lastType = type
            type = data['NamedEntityTag']
            if type!= 'O':
                entityWords += [word]
            elif len(entityWords):
                entities += [" ".join(entityWords)]
                entityWords = []
        if len(entityWords):
            entities += [" ".join(entityWords)]
        # now shove in all noun phrases
        parsed = nltk.tree.Tree.parse(sentence['parsetree'])
        entities += [' '.join(f.leaves()) for f in parsed.subtrees() if f.node == u'NP']
    return list(set(entities))

def expand_entities(entities, wiki_url):
    if len(entities) == 0:
        return {}
    r = requests.get(wiki_url.replace('http://', 'http://sandbox-s2.')+"/wikia.php", params={'controller':'WikiaSearchController', 'method':'resolveEntities', 'entities':'|'.join(entities)})
    try:
        return json.loads(r.content)
    except ValueError:
        return {}
