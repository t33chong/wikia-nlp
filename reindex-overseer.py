"""
Fires off a reindex command using our existing scribe infrastructure by mocking scribe events
"""
from optparse import OptionParser
from WikiaSolr import get_config, ScribeOverseer, ReindexOverseer

parser = OptionParser()
parser.add_option("-n", "--workers", dest="workers", action="store", default=10,
                  help="Specifies the number of open worker processes")
parser.add_option("-d", "--db", dest="dbconf", action="store", default="/usr/wikia/conf/current/DB.yml",
                  help="DB Config File")
parser.add_option("-f", "--file", dest="file", action="store", default=None,
                  help="A newline-separated list of wiki IDs")

(options, args) = parser.parse_args()


if (options.file):
    overseer = ReindexOverseer(get_config(), vars(options))
else:
    overseer = ScribeOverseer(get_config(), vars(options))

overseer.oversee()
