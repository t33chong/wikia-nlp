"""
Responsible for iterating over wikis and spawning processes.
"""

import json, socket
from optparse import OptionParser
from WikiaSolr import NLPOverseer

nlp_config = json.loads(open('nlp-config.json').read())[socket.gethostname()]
workers = nlp_config['workers']

parser = OptionParser()
parser.add_option("-n", "--workers", dest="workers", action="store", default=workers,
                  help="Specifies the number of open worker processes")
parser.add_option("-w", "--wam-threshold", dest="wam_threshold", action="store", default=None,
                  help="If we have a threshold, we restart our wiki iteration when we hit it")
parser.add_option("-f", "--filter-query", dest="filterQuery", action="store", default=None,
                  help="Filter queries can be used to slice and dice what we're iterating over")
parser.add_option("-s", "--sort", dest="sort", action="store", default="wam_i desc",
                  help="Sorting groups lets us prioritize which wikis we act on first")
parser.add_option("-r", "--start", dest="start", action="store", default=0,
                  help="Index at which to start iterating over wikis")
parser.add_option("-m", "--modulo", dest="modulo", action="store",
                  help="Remainder when wid is divided by 2; determines odd/even")
parser.add_option("-l", "--language", dest="language", action="store", default="en",
                  help="Language to limit wiki parsing")
#TODO: make last_indexed True as default
parser.add_option("-i", "--last-indexed", dest="last_indexed", action="store", default=0,
                  help="0 or 1; start writing files since last indexed?")

(options, args) = parser.parse_args()

config = json.loads("".join(open('worker-config.json').readlines()))
NLPOverseer(config, vars(options)).oversee()
