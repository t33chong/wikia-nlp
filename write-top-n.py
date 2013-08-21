import os
from gzip import GzipFile
from WikiaSolr import QueryIterator, get_config

def write_files(wid):
    filepath = os.path.join('html_en', str(wid))
    if not os.path.exists(filepath):
        os.makedirs(filepath)
    qi = QueryIterator(get_config(), {'query': 'wid:%s AND iscontent:true' % str(wid), 'fields': 'pageid, html_en', 'sort': 'id asc'})
    for doc in qi:
        print 'extracting words for %i_%i...' % (wid, doc['pageid'])
        page_file = GzipFile(os.path.join(filepath, '%s.gz' % doc['pageid']), 'w', compresslevel=9)
        page_file.write(doc.get('html_en', '').lower().encode('utf-8'))
        page_file.close()

def main():
    for line in open('top10'):
        write_files(int(line.strip()))

if __name__ == '__main__':
    main()
