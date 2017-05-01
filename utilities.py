from __future__ import print_function
from __future__ import unicode_literals
import re
import logging
import os
from collections import namedtuple

from pymongo import MongoClient

try:
    from ConfigParser import ConfigParser
except ImportError:
    from configparser import ConfigParser

global logger


def parse_config(config_filename):
    """
    Parse the config file and return relevant information.

    Parameters
    ----------

    config_filename: String.
                        Path to config file.

    Returns
    -------

    server_list: Named tuple.
                    Config information specifically related to the remote
                    server for FTP uploading.

    geo_list : Named tuple.
                 Config information for geocoding.

    file_list: Named tuple.
                All the other config information not in ``server_list``.

    petrarch_version: Int
                Either 1 or 2, indicating whether Petrarch or Petrarch2 should be used.
    """
    parser = ConfigParser()
    parser.read(config_filename)

    print('Found a config file in working directory')
    try:
        serv_name = parser.get('Server', 'server_name')
        username = parser.get('Server', 'username')
        password = parser.get('Server', 'password')
        server_dir = parser.get('Server', 'server_dir')

        server_attrs = namedtuple('ServerAttributes', ['serv_name',
                                                       'username',
                                                       'password',
                                                       'server_dir'])
        server_list = server_attrs(serv_name, username, password,
                                   server_dir)

        geo_service = parser.get('Geolocation', 'geo_service')
        cliff_host = parser.get('Geolocation', 'cliff_host')
        cliff_port = parser.get('Geolocation', 'cliff_port')
        mordecai_host = parser.get('Geolocation', 'mordecai_host')
        mordecai_port = parser.get('Geolocation', 'mordecai_port')


        geo_attrs = namedtuple('GeolocationAttributes', ['geo_service',
                                                         'cliff_host',
                                                         'cliff_port',
                                                         'mordecai_host',
                                                         'mordecai_port'
                                                       ])

        geo_list = geo_attrs(geo_service, cliff_host, cliff_port,
                                     mordecai_host, mordecai_port)


        # these are listed in the order generated
        scraper_stem = parser.get('Pipeline', 'scraper_stem')
        recordfile_stem = parser.get('Pipeline', 'recordfile_stem')
        fullfile_stem = parser.get('Pipeline', 'fullfile_stem')
        eventfile_stem = parser.get('Pipeline', 'eventfile_stem')
        dupfile_stem = parser.get('Pipeline', 'dupfile_stem')
        outputfile_stem = parser.get('Pipeline', 'outputfile_stem')
        oneaday_filter = parser.get('Pipeline', 'oneaday_filter')
        if 'Auth' in parser.sections():
            auth_db = parser.get('Auth', 'auth_db')
            auth_user = parser.get('Auth', 'auth_user')
            auth_pass = parser.get('Auth', 'auth_pass')
            db_host = parser.get('Auth', 'db_host')
        else:
            auth_db = ''
            auth_user = ''
            auth_pass = ''
            db_host = os.getenv('MONGO_HOST') or None
        if 'Logging' in parser.sections():
            log_file = parser.get('Logging', 'log_file')
        else:
            log_file = ''

        petrarch_version = parser.get('Petrarch', 'petrarch_version')

        if 'Mongo' in parser.sections():
            db_db = parser.get('Mongo', 'db')
            db_collection = parser.get('Mongo', 'collection')
        else:
            db_db = 'event_scrape'
            db_collection = 'stories'



        file_attrs = namedtuple('FileAttributes', ['scraper_stem',
                                                   'recordfile_stem',
                                                   'fullfile_stem',
                                                   'eventfile_stem',
                                                   'dupfile_stem',
                                                   'outputfile_stem',
                                                   'oneaday_filter',
                                                   'log_file',
                                                   'auth_db',
                                                   'auth_user',
                                                   'auth_pass',
                                                   'db_host',
                                                   'db_db',
                                                   'db_collection'])

        file_list = file_attrs(scraper_stem, recordfile_stem, fullfile_stem,
                               eventfile_stem, dupfile_stem, outputfile_stem,
                               oneaday_filter, log_file, auth_db, auth_user,
                               auth_pass, db_host, db_db, db_collection)



        return server_list, geo_list, file_list, petrarch_version
    except Exception as e:
        print('Problem parsing config file. {}'.format(e))


def init_logger(logger_filename):
    """
    Initialize a log file.

    Parameters
    ----------

    logger_filename: String.
                        Path to the log file.
    """

    logger = logging.getLogger('pipeline_log')
    logger.setLevel(logging.INFO)

    fh = logging.FileHandler(logger_filename, 'w')
    formatter = logging.Formatter('%(levelname)s %(asctime)s: %(message)s')
    fh.setFormatter(formatter)

    logger.addHandler(fh)
    logger.info('Running')

    logger.info('PHOX.pipeline run')


def do_RuntimeError(st1, filename='', st2=''):
    """
    This is a general routine for raising the RuntimeError: the reason to make
    this a separate procedure is to allow the error message information to be
    specified only once. As long as it isn't caught explicitly, the error
    appears to propagate out to the calling program, which can deal with it.
    """
    logger = logging.getLogger('pipeline_log')
    print(st1, filename, st2)
    logger.error(st1 + ' ' + filename + ' ' + st2 + '\n')
    raise RuntimeError(st1 + ' ' + filename + ' ' + st2)


def make_conn(db_db, db_collection, db_auth, db_user, db_pass, db_host=None):
    """
    Function to establish a connection to a local MonoDB instance.


    Parameters
    ----------

    db_auth: String.
                MongoDB database that should be used for user authentication.

    db_user: String.
                Username for MongoDB authentication.

    db_user: String.
                Password for MongoDB authentication.

    Returns
    -------

    collection: pymongo.collection.Collection.
                Collection within MongoDB that holds the scraped news stories.

    """

    if db_host:
        client = MongoClient(db_host)
    else:
        client = MongoClient()
    if db_auth:
        client[db_auth].authenticate(db_user, db_pass)
    database = client[db_db]
    collection = database[db_collection]
    return collection


def sentence_segmenter(paragr):
    """
    Function to break a string 'paragraph' into a list of sentences based on
    the following rules:

    1. Look for terminal [.,?,!] followed by a space and [A-Z]
    2. If ., check against abbreviation list ABBREV_LIST: Get the string
    between the . and the previous blank, lower-case it, and see if it is in
    the list. Also check for single-letter initials. If true, continue search
    for terminal punctuation
    3. Extend selection to balance (...) and "...". Reapply termination rules
    4. Add to sentlist if the length of the string is between MIN_SENTLENGTH
    and MAX_SENTLENGTH
    5. Returns sentlist

    Parameters
    ----------

    paragr: String.
            Content that will be split into constituent sentences.

    Returns
    -------

    sentlist: List.
                List of sentences.

    """
    # this is relatively high because we are only looking for sentences that
    # will have subject and object
    MIN_SENTLENGTH = 100
    MAX_SENTLENGTH = 512

    # sentence termination pattern used in sentence_segmenter(paragr)
    terpat = re.compile('[\.\?!]\s+[A-Z\"]')

    # source: LbjNerTagger1.11.release/Data/KnownLists/known_title.lst from
    # University of Illinois with editing
    ABBREV_LIST = ['mrs.', 'ms.', 'mr.', 'dr.', 'gov.', 'sr.', 'rev.', 'r.n.',
                   'pres.', 'treas.', 'sect.', 'maj.', 'ph.d.', 'ed. psy.',
                   'proc.', 'fr.', 'asst.', 'p.f.c.', 'prof.', 'admr.',
                   'engr.', 'mgr.', 'supt.', 'admin.', 'assoc.', 'voc.',
                   'hon.', 'm.d.', 'dpty.',  'sec.', 'capt.', 'c.e.o.',
                   'c.f.o.', 'c.i.o.', 'c.o.o.', 'c.p.a.', 'c.n.a.', 'acct.',
                   'llc.', 'inc.', 'dir.', 'esq.', 'lt.', 'd.d.', 'ed.',
                   'revd.', 'psy.d.', 'v.p.',  'senr.', 'gen.', 'prov.',
                   'cmdr.', 'sgt.', 'sen.', 'col.', 'lieut.', 'cpl.', 'pfc.',
                   'k.p.h.', 'cent.', 'deg.', 'doz.', 'Fahr.', 'Cel.', 'F.',
                   'C.', 'K.', 'ft.', 'fur.',  'gal.', 'gr.', 'in.', 'kg.',
                   'km.', 'kw.', 'l.', 'lat.', 'lb.', 'lb per sq in.', 'long.',
                   'mg.', 'mm.,, m.p.g.', 'm.p.h.', 'cc.', 'qr.', 'qt.', 'sq.',
                   't.', 'vol.',  'w.', 'wt.']

    sentlist = []
    # controls skipping over non-terminal conditions
    searchstart = 0
    terloc = terpat.search(paragr)
    while terloc:
        isok = True
        if paragr[terloc.start()] == '.':
            if (paragr[terloc.start() - 1].isupper() and
                    paragr[terloc.start() - 2] == ' '):
                        isok = False      # single initials
            else:
                # check abbreviations
                loc = paragr.rfind(' ', 0, terloc.start() - 1)
                if loc > 0:
                    if paragr[loc + 1:terloc.start() + 1].lower() in ABBREV_LIST:
                        isok = False
        if paragr[:terloc.start()].count('(') != paragr[:terloc.start()].count(')'):
            isok = False
        if paragr[:terloc.start()].count('"') % 2 != 0:
            isok = False
        if isok:
            if (len(paragr[:terloc.start()]) > MIN_SENTLENGTH and
                    len(paragr[:terloc.start()]) < MAX_SENTLENGTH):
                sentlist.append(paragr[:terloc.start() + 2])
            paragr = paragr[terloc.end() - 1:]
            searchstart = 0
        else:
            searchstart = terloc.start() + 2

        terloc = terpat.search(paragr, searchstart)

    # add final sentence
    if (len(paragr) > MIN_SENTLENGTH and len(paragr) < MAX_SENTLENGTH):
        sentlist.append(paragr)

    return sentlist
