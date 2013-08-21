# -*- coding: utf-8 *-*
"""
Polls the scribe queue and puts events into Mongo
"""
import pymongo, os, json, datetime

config = json.loads("".join(open('worker-config.json').readlines()))
connection = pymongo.Connection(config["logging"]["host"], config["logging"]["port"])
db = connection.scribe.events
scribepath = config["scribe"]["path"]

while True:
    for subfolder in config["scribe"]["priority"].keys():
        currpath = scribepath+subfolder
        for filename in os.listdir(currpath):
            fname = currpath+"/"+filename
            try:
                with open(fname) as f:
                    extras = {
                        "timestamp": datetime.datetime.utcnow(),
                         "priority": config["scribe"]["priority"][subfolder],
                        "available": 1
                    }
                    objects = [dict(loaded.items() + extras.items()) for loaded in json.loads("["+",".join(f.readlines())+"]")];
                    if len(objects) > 0:
                        db.insert(objects)
                os.remove("%s/%s" % (currpath, filename))
            except:
                pass
