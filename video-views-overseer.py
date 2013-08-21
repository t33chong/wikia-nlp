"""
Iterates through wikis with videos, and fires off processes to update the video views
"""
from optparse import OptionParser
from WikiaSolr import VideoViewsOverseer, get_config
import json

parser = OptionParser()
parser.add_option("-n", "--workers", dest="workers", action="store", default=10,
                  help="Specifies the number of open worker processes")
parser.add_option("-w", "--wam-threshold", dest="wam_threshold", action="store", default=None,
                  help="If we have a threshold, we restart our wiki iteration when we hit it")
parser.add_option("-f", "--filter-query", dest="filterQuery", action="store", default=None,
                  help="Filter queries can be used to slice and dice what we're iterating over")
parser.add_option("-s", "--sort", dest="sort", action="store", default="wam desc",
                  help="Sorting groups lets us prioritize which wikis we act on first")
parser.add_option("-t", "--threads", dest="threads", action="store", default="4",
                  help="Number of map-reduce threads per worker")

(options, args) = parser.parse_args()

VideoViewsOverseer(get_config(), vars(options)).oversee()
