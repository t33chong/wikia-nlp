import os
import shutil
import re
import time
from subprocess import Popen
from collections import defaultdict
from corenlp.corenlp import init_corenlp_command

CORENLP_PATH = '/home/tristan/stanford-corenlp-python/stanford-corenlp-full-2013-06-20'

def ensure_dir_exists(directory):
    """
    Makes sure the directory given as an argument exists, and returns the same
    directory.
    """
    if not os.path.exists(directory):
        os.makedirs(directory)
    return directory

class BatchParser(object):
    def __init__(self, text_dir, memory, properties, threads):
        """
        Class for calling the Stanford CoreNLP suite on batches of text pulled
        from a given wiki.
        """
        self.text_dir = text_dir
        self.wid = os.path.basename(text_dir)
        self.memory = memory
        self.properties = properties
        self.xml_dir = ensure_dir_exists('/data/xml/' + self.wid)
        self.threads = threads
        self.staging_dir = ensure_dir_exists('/data/staging/' + self.wid)
        self.retry_dir = ensure_dir_exists('/data/retry/' + self.wid)
        self.processes = {}
        self.time = {}
        self.filelist_index = {}
        self.filelistpath = ensure_dir_exists('/data/filelist/' + self.wid)
        self._write_initial_filelists()

    def get_existing_xml_files(self, directory):
        """
        Returns a dict containing pageids of existing XML files as keys, and True
        as values, within a specified directory.
        """
        preexisting = {}
        if os.path.exists(directory):
            for (dirpath, dirnames, filenames) in os.walk(directory):
                for filename in filenames:
                    pageid = re.match('[^\.]+', filename).group(0)
                    preexisting[pageid] = True
        return preexisting

    def get_all_existing_xml_files(self):
        """
        Returns a dict containing pageids of existing XML files as keys, and True
        as values, across staging_dir and xml_dir directories.
        """
        return dict(self.get_existing_xml_files(self.xml_dir).items() +
                    self.get_existing_xml_files(self.staging_dir).items())

    def _write_initial_filelists(self):
        """
        Called by constructor. Write filelists for all files in the text_dir
        (split by modulo = # of threads), and store their locations in dict
        self.filelist_index, with process ID int i as key.
        """
        modulo = self.threads

        preexisting = self.get_all_existing_xml_files()
        filelists = defaultdict(list)
        n = 0
        for (dirpath, dirnames, filenames) in os.walk(self.text_dir):
            for filename in filenames:
                if not preexisting.get(filename, False):
                    filepath = os.path.join(dirpath, filename)
                    filelists[n % modulo].append(filepath)
                    n += 1
        for i in filelists:
            filelist = os.path.join(self.filelistpath, str(i))
            with open(filelist, 'w') as f:
                f.write('\n'.join(filelists[i]))
            self.filelist_index[i] = filelist

    def _get_batch_command(self, i):
        """
        Takes process ID int i as a parameter. Retrieves and returns the
        appropriate command to call the Java parser on batch i.
        """
        corenlp_command = init_corenlp_command(CORENLP_PATH, self.memory, self.properties)
        output_directory = ensure_dir_exists(os.path.join(self.staging_dir, str(i)))
        return '%s -filelist %s -outputDirectory %s' % (corenlp_command, self.filelist_index[i], output_directory)

    def open_process(self, i):
        """
        Called by parse(). Takes process ID int i as a parameter, and calls a new
        parser process, storing the appropriate info in self.processes and
        self.time.
        """
        command = self._get_batch_command(i)
        print '%i: "%s"' % (i, command)
        self.time[i] = time.time()
        self.processes[i] = Popen([command], shell=True, preexec_fn=os.setsid)

    def is_parse_incomplete(self, i):
        """
        Check whether all text files in filelist i have corresponding XML files as
        a result of a successful parse.
        If parse is incomplete, method writes a new filelist for remaining text
        files, and returns an integer corresponding to the new filelist_index.
        If parse is complete, returns False.
        """
        complete = self.get_existing_xml_files(os.path.join(self.staging_dir, str(i)))
        incomplete = []
        for line in open(self.filelist_index[i]):
            pageid = os.path.basename(line.strip())
            if not complete.get(pageid, False):
                incomplete.append(line.strip())
        if incomplete:
            j = max(self.filelist_index.keys()) + 1
            retry_files = []
            retry_path = ensure_dir_exists(os.path.join(self.retry_dir, str(j)))
            for src_path in incomplete:
                dest_path = os.path.join(retry_path, os.path.basename(src_path))
                retry_files.append(dest_path)
                shutil.move(src_path, dest_path)
            filelist = os.path.join(self.filelistpath, str(j))
            with open(filelist, 'w') as f:
                f.write('\n'.join(retry_files))
            self.filelist_index[j] = filelist
            return j
        return False

    def clean_up(self):
        """
        Deletes text_dir and filelistpath. Moves all XML files from staging_dir
        to xml_dir.
        """
        # text_dir and filelistpath removal temporarily disabled for debugging
        #shutil.rmtree(self.text_dir)
        #shutil.rmtree(self.filelistpath)
        for (dirpath, dirnames, filenames) in os.walk(self.staging_dir):
            for filename in filenames:
                src_path = os.path.join(dirpath, filename)
                first_digit = filename[0]
                first_digit_dir = ensure_dir_exists(os.path.join(self.xml_dir, first_digit))
                dest_path = os.path.join(first_digit_dir, filename)
                shutil.move(src_path, dest_path)
        shutil.rmtree(self.staging_dir)

    def parse(self):
        """
        Manages subprocesses responsible for parsing subdirectories of text
        param threads: number of concurrent threads, default 2
        """
        for i in range(self.threads):
            self.open_process(i)
        while True:
            if len(self.processes) == 0:
                break
            # sleep to avoid looping incessantly
            time.sleep(5)
            for i in self.processes.keys():
                # TODO: change 'is not' to '!='
                if self.processes[i].poll() is not None:
                    print '==================== wid %s i %i IS NOT NONE ====================' % (self.wid, i)
                    del self.processes[i]
                    j = self.is_parse_incomplete(i)
                    print '==================== wid %s j = %s ====================' % (self.wid, str(j))
                    if j:
                        print '==================== wid %s OPENING RETRY PROCESS ====================' % self.wid
                        self.open_process(j)

        self.clean_up()

        return self.xml_dir

if __name__ == '__main__':
    pass
