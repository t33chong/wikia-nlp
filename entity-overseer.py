"""
Responsible for iterating over wids in the XML directory and writing entity data
collected from harvester subprocesses to file.
"""

import json, socket
from optparse import OptionParser
from WikiaSolr import EntityOverseer

nlp_config = json.loads(open('nlp-config.json').read())[socket.gethostname()]
workers = nlp_config['workers']

parser = OptionParser()
parser.add_option("-v", "--verbose", dest="verbose", action="store", default=True,
                  help="Shows verbose output")
parser.add_option("-n", "--workers", dest="workers", action="store", default=workers,
                  help="Specifies the number of open worker processes")
parser.add_option("-c", "--csv-file", dest="csv_file", action="store", default='/data/wikis_to_entities.csv',
                  help="Defines the output CSV file")
parser.add_option("-x", "--xml-dir", dest="xml_dir", action="store", default='/data/xml',
                  help="Defines the XML directory to iterate over")

(options, args) = parser.parse_args()

EntityOverseer(vars(options)).oversee()
