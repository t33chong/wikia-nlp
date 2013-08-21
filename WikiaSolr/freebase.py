import json, requests

# todo: handle this better, this is my own key so PPLZ don't abuse it -- robert
freebase_api_key = "AIzaSyDjHJ71DzV7cmzWzOZzY_XcnqqrhHvroC0"

def freebase_entity(entity):
    junk = (entity, None)
    params = {'query':entity, 'key':freebase_api_key}
    r = requests.get('https://www.googleapis.com/freebase/v1/search/', params=params)
    if not r.content:
        return junk
    results = json.loads(r.content)['result']
    if not len(results):
        return junk
    return (entity, results[0])


def freebase_topics(id):
    junk = (topic, None)
    params = {'key':freebase_api_key, 'filter':'suggest'}
    url = 'https://www.googleapis.com/freebase/v1/topic/%s' % id
    r = requests.get(url, params=params)
    if not r.content:
        return junk
    return (id, json.loads(r.content))
