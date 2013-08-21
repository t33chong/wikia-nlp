#!/usr/bin/python
"""
gets current JVM memory usage, and if it's too high for too long, restarts solr.
run this as root.
"""
import requests, json, time, math, datetime, sys, socket, subprocess

highJvmUsageStart = 0
while True:
    try:
        url = "http://%s.prod.wikia.net:8983/solr/main/admin/system?wt=json" % socket.gethostname()
        r = requests.get(url)
        d = json.loads(r.content)
    except:
        # solr may just be restarting
        print sys.exc_info()
        time.sleep(60)
        continue

    curtime = time.time()
    usedPct = d['jvm']['memory']['raw']['used%']
    if usedPct >= 90 and highJvmUsageStart == 0:
        highJvmUsageStart = curtime
    else:
        highJvmUsageStart = 0

    if highJvmUsageStart > 0 and curtime - highJvmUsageStart >= 600: # 10 minutes
        print "Restarting Solr...",
        print subprocess.call("service solr restart", shell=True)

    print "JVM usage: %.2f%%, running for %dms" % (usedPct, d['jvm']['jmx']['upTimeMS'])

    time.sleep(30)

