from WikiaSolr import CrossWikiReindexOverseer, get_config
import sys

offset = 0 if len(sys.argv) == 2 else sys.argv[2]

CrossWikiReindexOverseer(get_config(), {'workers':sys.argv[1], 'offset':offset}).oversee()
