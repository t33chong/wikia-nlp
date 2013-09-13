"""
Responsible for writing text files, either for single documents, or for all content
articles in a given wiki.
Can write to a local machine or to an Amazon AWS S3 bucket.

REMOVE THE FOLLOWING:
Wiki ID is provided as sys.argv[1]
Language is optionally provided as sys.argv[2]
Last indexed condition is optionally provided as sys.argv[3]
"""
import os
import requests
from . import QueryIterator
from utils import clean_list, ensure_dir_exists

HOST = 'http://search-s11.prod.wikia.net:8983/solr/main/select'

class TextWriter(object):
    def __init__(self, lang='en', local=False, destination='/data'):
        self.language = lang
        self.destination = ensure_dir_exists(destination)
        if not local:
            aws = json.loads(open('aws_credentials.json').read())
            self.aws_key = aws['key']
            self.aws_secret = aws['secret']
            self.destination = """Implement writing to S3"""

    def get_single_doc(self, wid_or_id):
        if '_' in str(wid_or_id):
            return json.loads(requests.get(HOST, params={'q': 'id:%s' % wid_or_id, 'fl': 'html_en', 'wt': 'json'}).content)['response']['docs'][0].get('html_%s' % self.language, '')

    def write_to_aws(self, text):
        pass

    def write_to_local(self, text):
        pass

#DATA_DIR = '/data' # /data
#
#wid = int(sys.argv[1])
#language = 'en' if len(sys.argv) < 3 else sys.argv[2]
##TODO: make last_indexed True as default
#last_indexed = False if len(sys.argv) < 4 else bool(int(sys.argv[3]))
#threads = config['threads']
#
#def write_text(wid):
#    try:
#        last_indexed_value = open(os.path.join(DATA_DIR, 'last_indexed.txt')).read().strip()
#    except IOError:
#        last_indexed_value = '2000-01-01T12:00:00.000Z'
#    text_dir = os.path.join(DATA_DIR, 'text', str(wid))
#    query = 'wid:%s AND iscontent:true' % str(wid)
#    if last_indexed:
#        query += ' AND indexed:["%s" TO *]' % last_indexed_value
#    qi = QueryIterator(get_config(), {'query': query, 'fields': 'pageid, html_en, indexed', 'sort': 'pageid asc'})
#    print 'Writing text from %s to file...' % str(wid)
#    for doc in qi:
#        pageid = doc['pageid']
#        text = '\n'.join(clean_list(doc.get('html_%s' % language, '')))
#        last_indexed_value = max(last_indexed_value, doc.get('indexed'))
#        text_subdir = os.path.join(text_dir, str(pageid)[0])
#        if not os.path.exists(text_subdir):
#            os.makedirs(text_subdir)
#        text_filepath = os.path.join(text_subdir, str(pageid))
#        with open(text_filepath, 'w') as text_file:
#            text_file.write(text)
#    with open(os.path.join(DATA_DIR, 'last_indexed.txt'), 'w') as last_indexed_file:
#        last_indexed_file.write(last_indexed_value)
#    return text_dir
#
#def convert_xml_to_gzip(xml_dir):
#    for subdir in os.listdir(xml_dir):
#        subdir_path = os.path.join(xml_dir, subdir)
#        for xml_file in os.listdir(subdir_path):
#            if xml_file.endswith('.xml'):
#                xml_path = os.path.join(subdir_path, xml_file)
#                gzip_filepath = xml_path + '.gz'
#                gzip_file = gzip.GzipFile(gzip_filepath, 'w')
#                gzip_file.write(open(xml_path).read())
#                gzip_file.close()
#                os.remove(xml_path)
#
#def main():
#    start_time = time.time()
#
#    text_dir = write_text(wid)
#    xml_dir = BatchParser(text_dir, MEMORY, PROPERTIES, threads).parse()
#
#    convert_xml_to_gzip(xml_dir)
#
#    total_time = time.time() - start_time
#
#    time_dir = ensure_dir_exists('/data/time')
#    time_file_name = os.path.join(time_dir, str(wid))
#    with open(time_file_name, 'w') as time_file:
#        time_file.write(str(total_time))
#
#if __name__ == '__main__':
#    main()
