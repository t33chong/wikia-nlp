"""
Responsible for parsing and writing files in XML format for all content pages on a per-wiki basis.

Wiki ID is provided as sys.argv[1]
Number of threads is optionally provided as sys.argv[2]
"""
import sys, json, gzip, tempfile, requests
from corenlp.corenlp import *
from WikiaSolr import QueryIterator, get_config, ParserOverseer

DATA_DIR = '/data' # /data

# CORENLP CONSTANTS
CORENLP_PATH = '/home/tristan/stanford-corenlp-python/stanford-corenlp-full-2013-06-20'
MEMORY = '3g'
PROPERTIES = '/home/tristan/stanford-corenlp-python/corenlp/default.properties'

wid = int(sys.argv[1])
language = 'en' if len(sys.argv) < 3 else sys.argv[2]
threads = 4 if len(sys.argv) < 4 else int(sys.argv[3])
#TODO: make last_indexed True as default
last_indexed = False if len(sys.argv) < 5 else bool(int(sys.argv[4]))

def write_text(wid):
    try:
        last_indexed_value = open(os.path.join(DATA_DIR, 'last_indexed.txt')).read().strip()
    except IOError:
        last_indexed_value = '2000-01-01T12:00:00.000Z'
    text_dir = os.path.join(DATA_DIR, 'text', str(wid))
    query = 'wid:%s AND iscontent:true' % str(wid)
    if last_indexed:
        query += ' AND indexed:["%s" TO *]' % last_indexed_value
    qi = QueryIterator(get_config(), {'query': query, 'fields': 'pageid, html_en, indexed', 'sort': 'pageid asc'})
#    doc_count = 0
#    batch_count = 0
    for doc in qi:
#        if doc_count % 100 == 0:
#            try:
#                filelist.close()
#            except NameError:
#                pass
#            batch_count += 1
#            filelist_path = os.path.join(DATA_DIR, 'filelist', str(wid))
#            if not os.path.exists(filelist_path):
#                os.makedirs(filelist_path)
#            filelist = open(os.path.join(filelist_path, 'batch%i' % batch_count), 'w')
        pageid = doc['pageid']
        text = doc.get('html_%s' % language, '').encode('utf-8')
        last_indexed_value = max(last_indexed_value, doc.get('indexed'))
        print 'writing text from %s_%s to file...' % (str(wid), str(pageid))
        text_subdir = os.path.join(text_dir, str(pageid)[0])
        if not os.path.exists(text_subdir):
            os.makedirs(text_subdir)
        text_filepath = os.path.join(text_subdir, '%s.gz' % pageid)
        text_file = gzip.GzipFile(text_filepath, 'w')
        text_file.write(text)
        text_file.close()
#        filelist.write(text_filepath + '\n')
#        doc_count += 1
    with open(os.path.join(DATA_DIR, 'last_indexed.txt'), 'w') as last_indexed_file:
        last_indexed_file.write(last_indexed_value)

def write_filelists(wid):
    subdirectories = []
    text_dir = os.path.join(DATA_DIR, 'text', str(wid))
    for subdir_num in os.listdir(text_dir):
        text_subdir = os.path.join(text_dir, subdir_num)
        files = [os.path.join(text_subdir, f) for f in os.listdir(text_subdir)]
        filelist_dir = os.path.join(DATA_DIR, 'filelist', str(wid))
        filelist_file = os.path.join(filelist_dir, str(subdir_num))
        with open(filelist_file, 'w') as filelist:
            filelist.write('\n'.join(files))
        output_directory = os.path.join(DATA_DIR, 'xml', str(wid), subdir_num)
        subdirectories.append({'index': '%s_%s' % (str(wid), subdir_num), 'command': init_corenlp_command(CORENLP_PATH, MEMORY, PROPERTIES), 'filelist': filelist_file, 'outputDirectory': output_directory})
    return subdirectories

#TODO
"""
- implement batch parser as part of this script, not in stanford-corenlp-python, to read text .gz files and write to xml .gz files
- use multiprocess logic in overseer.py for above task
- delete temporary text .gz and filelist files
"""

def main():
    write_text(wid)
    subdirectories = write_filelists(wid)
    ParserOverseer(subdirectories, threads=2).oversee()

if __name__ == '__main__':
    main()
