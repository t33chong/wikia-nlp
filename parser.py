import os
import re
import time
from subprocess import Popen
from collections import defaultdict
from corenlp.corenlp import init_corenlp_command

CORENLP_PATH = '/home/tristan/stanford-corenlp-python/stanford-corenlp-full-2013-06-20'

def ensure_dir_exists(directory):
    """
    Makes sure the directory given as an argument exists.
    Returns the same directory.
    """
    if not os.path.exists(directory):
        os.makedirs(directory)
    return directory

class BatchParser(object):
    def __init__(self, text_dir, memory, properties, threads):
        self.text_dir = text_dir
        self.wid = os.path.basename(text_dir)
        self.memory = memory
        self.properties = properties
        self.xml_dir = ensure_dir_exists('/data/xml/' + self.wid)
        self.threads = threads
        self.staging_dir = ensure_dir_exists('/data/staging/' + self.wid)
        self.processes = {}
        self.time = {}

    def get_existing_xml_files(self, directory):
        preexisting = {}
        if os.path.exists(directory):
            for (dirpath, dirnames, filenames) in os.walk(self.xml_dir):
                for filename in filenames:
                    pageid = re.match('[^\.]+', filename).group(0)
                    preexisting[pageid] = True
        return preexisting

    def get_all_existing_xml_files(self):
        return dict(self.get_existing_xml_files(self.text_dir).items() +
                    self.get_existing_xml_files(self.staging_dir).items())

    def write_filelists(self, retry):
        """
        If writing filelists for the original text directory, retry is False, and
        filenames will be written to different filelists using a modulo equal to
        the number of threads. Returns a list of filelist filepaths.

        If writing filelists for a retry directory, retry is True, and filenames
        will be written to a single filelist. Returns a list of length 1,
        containing the filelist filepath.
        """
        modulo = self.threads
        if retry:
            modulo = 1
        preexisting = self.get_all_existing_xml_files()
        filelists = defaultdict(list)
        n = 0
        for (dirpath, dirnames, filenames) in os.walk(self.text_dir):
            for filename in filenames:
                if not preexisting.get(filename, False):
                    filepath = os.path.join(dirpath, filename)
                    filelists[n % modulo].append(filepath)
                    n += 1
        filelistpath = ensure_dir_exists('/data/filelist/' + self.wid)
        filelist_files = []
        for filelist in filelists:
            filelist_file = os.path.join(filelistpath, str(filelist))
            if retry:
                filelist_file  = os.path.join(filelistpath, str(max(os.listdir(filelistpath)) + 1))
            with open(filelist_file, 'w') as f:
                f.write('\n'.join(filelists[filelist]))
            filelist_files.append(filelist_file)
        return filelist_files

    def get_batch_command(self, retry):
        corenlp_command = init_corenlp_command(CORENLP_PATH, self.memory, self.properties)
        if retry:
            filelist = self.write_filelists(True)[0]
            retry_dir = ensure_dir_exists('/data/retry/' + self.wid + '/' + os.path.basename(filelist))
            return '%s -filelist %s -outputDirectory %s' % (corenlp_command, filelist, retry_dir)
        for (i, filelist) in enumerate(self.write_filelists(retry)):
            yield '%s -filelist %s -outputDirectory %s' % (corenlp_command, filelist, os.path.join(self.staging_dir, str(i)))

    def open_process(self, i, retry=False):
        command = self.get_batch_command(retry)
        print '%i: "%s"' % (i, command)
        self.time[i] = time.time()
        self.processes[i] = Popen([command], shell=True, preexec_fn=os.setsid)

    def check_for_incomplete_parse(self):
        # TODO
        pass

    def parse(self, threads=2):
        """
        Manages subprocesses responsible for parsing subdirectories of text
        param threads: number of concurrent threads, default 2
        """
        for i in range(num_threads):
            self.open_process(i)
        while True:
            if len(self.processes) == 0:
                break
            # sleep to avoid looping incessantly
            time.sleep(5)
            for i in self.processes.keys():
                if self.processes[i].poll() is not None:
                    del self.processes[i]

                    # TODO: if there remain files in the filelist for which
                    # corresponding parsed xml files do not exist (either in the
                    # staging dir or the xml dir), write a new filelist consisting
                    # of the remaining files and open a process to parse it

                # TODO: for a given process, if all files in the input text_dir
                # have corresponding xml files in the staging dir, move the xml
                # files from the staging dir to the xml_dir

        return self.xml_dir

if __name__ == '__main__':
    pass
