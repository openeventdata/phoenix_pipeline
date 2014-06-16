from __future__ import print_function
from __future__ import unicode_literals
import logging
from collections import namedtuple

try:
    from ConfigParser import ConfigParser
except ImportError:
    from configparser import ConfigParser

global logger


def parse_config(config_filename):
    """
    Parse config_filename and put the resulting ftp directory information in
    Server_List and the various file name stems in the named globals. This is
    called once at the beginning of the pipeline to extract the information,
    afterwhich the various routines use phox_utilities.<var>.
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

        # these are listed in the order generated
        scraper_stem = parser.get('Pipeline', 'scraper_stem')
        recordfile_stem = parser.get('Pipeline', 'recordfile_stem')
        fullfile_stem = parser.get('Pipeline', 'fullfile_stem')
        eventfile_stem = parser.get('Pipeline', 'eventfile_stem')
        dupfile_stem = parser.get('Pipeline', 'dupfile_stem')
        outputfile_stem = parser.get('Pipeline', 'outputfile_stem')
        oneaday_filter = parser.get('Pipeline', 'oneaday_filter')
        if 'Logging' in parser.sections():
            log_file = parser.get('Logging', 'log_file')
        else:
            log_file = ''

        file_attrs = namedtuple('FileAttributes', ['scraper_stem',
                                                   'recordfile_stem',
                                                   'fullfile_stem',
                                                   'eventfile_stem',
                                                   'dupfile_stem',
                                                   'outputfile_stem',
                                                   'oneaday_filter',
                                                   'log_file'])

        file_list = file_attrs(scraper_stem, recordfile_stem, fullfile_stem,
                               eventfile_stem, dupfile_stem, outputfile_stem,
                               oneaday_filter, log_file)

        return server_list, file_list
    except Exception as e:
        print('Problem parsing config file. {}'.format(e))


def init_logger(logger_filename):

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
