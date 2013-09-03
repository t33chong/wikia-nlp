"""
Responsible for parsing and writing files in XML format for all content pages on a per-wiki basis.

Wiki ID is provided as sys.argv[1]
Number of threads is optionally provided as sys.argv[2]
"""
import sys, shutil, json, gzip, tempfile, requests, time
from corenlp.corenlp import *
from corenlp.threadbatch import BatchParseThreader
from WikiaSolr import QueryIterator, get_config, ParserOverseer
from normalize import clean_list

DATA_DIR = '/data' # /data

# CORENLP CONSTANTS
CORENLP_PATH = '/home/tristan/stanford-corenlp-python/stanford-corenlp-full-2013-06-20'
MEMORY = '3g'
PROPERTIES = '/home/tristan/stanford-corenlp-python/corenlp/performance.properties'

wid = int(sys.argv[1])
language = 'en' if len(sys.argv) < 3 else sys.argv[2]
threads = 2 if len(sys.argv) < 4 else int(sys.argv[3])
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
    for doc in qi:
        pageid = doc['pageid']
        text = '\n'.join(clean_list(doc.get('html_%s' % language, '')))
        last_indexed_value = max(last_indexed_value, doc.get('indexed'))
        print 'writing text from %s_%s to file...' % (str(wid), str(pageid))
        text_subdir = os.path.join(text_dir, str(pageid)[0])
        if not os.path.exists(text_subdir):
            os.makedirs(text_subdir)
        text_filepath = os.path.join(text_subdir, str(pageid))
        with open(text_filepath, 'w') as text_file:
            text_file.write(text)
        #text_filepath = os.path.join(text_subdir, '%s.gz' % pageid)
        #text_file = gzip.GzipFile(text_filepath, 'w')
        #text_file.write(text)
        #text_file.close()
    with open(os.path.join(DATA_DIR, 'last_indexed.txt'), 'w') as last_indexed_file:
        last_indexed_file.write(last_indexed_value)
    return text_dir

def write_filelists(wid):
    subdirectories = []
    text_dir = os.path.join(DATA_DIR, 'text', str(wid))
    for subdir_num in os.listdir(text_dir):
        text_subdir = os.path.join(text_dir, subdir_num)
        files = [os.path.join(text_subdir, f) for f in os.listdir(text_subdir)]
        filelist_dir = os.path.join(DATA_DIR, 'filelist', str(wid))
        if not os.path.exists(filelist_dir):
            os.makedirs(filelist_dir)
        filelist_file = os.path.join(filelist_dir, str(subdir_num))
        with open(filelist_file, 'w') as filelist:
            filelist.write('\n'.join(files))
        output_directory = os.path.join(DATA_DIR, 'xml', str(wid), subdir_num)
        subdirectories.append({'index': '%s_%s' % (str(wid), subdir_num), 'command': init_corenlp_command(CORENLP_PATH, MEMORY, PROPERTIES), 'filelist': filelist_file, 'outputDirectory': output_directory})
    return filelist_dir, subdirectories

def convert_xml_to_gzip(subdirectories):
    for subdir in subdirectories:
        for xml_filename in os.listdir(subdir['outputDirectory']):
            xml_filepath = os.path.join(subdir['outputDirectory'], xml_filename)
            gzip_filepath = xml_filepath + '.gz'
            gzip_file = gzip.GzipFile(gzip_filepath, 'w')
            gzip_file.write(open(xml_filepath).read())
            gzip_file.close()
            os.remove(xml_filepath)

def main():
    start_time = time.time()
    text_dir = write_text(wid)
    #text_dir = '/data/text/831' # testing
    #filelist_dir, subdirectories = write_filelists(wid)
    output_directory = os.path.join(DATA_DIR, 'xml', str(wid))
    b = BatchParseThreader(text_dir, CORENLP_PATH, MEMORY, PROPERTIES, output_directory)
    b.parse(num_threads=2)
    #ParserOverseer(subdirectories, threads=2).oversee()
    #shutil.rmtree(text_dir)
    #shutil.rmtree(filelist_dir)
    #convert_xml_to_gzip(subdirectories)
    end_time = time.time()
    total_time = end_time - start_time
    time_dir = '/data/time'
    if not os.path.exists(time_dir):
        os.makedirs(time_dir)
    time_file_name = os.path.join(time_dir, str(wid))
    with open(time_file_name, 'w') as time_file:
        time_file.write(str(total_time))

if __name__ == '__main__':
    main()
