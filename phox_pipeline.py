import os
import sys
import glob
import logging
import datetime
import subprocess
import mongo_formatter
import oneaday_formatter
import scraper_connection
from ftplib import FTP
from ConfigParser import ConfigParser


def parse_config():
    """Function to parse the config file."""
    config_file = glob.glob('config.ini')
    parser = ConfigParser()
    if config_file:
        logger.info('Found a config file in working directory')
        parser.read(config_file)
        try:
            serv_name = parser.get('Server', 'server_name')
            username = parser.get('Server', 'username')
            password = parser.get('Server', 'password')
            server_dir = parser.get('Server', 'server_dir')

            scraper_stem = parser.get('Pipeline', 'scraper_stem')
            dupfile_stem = parser.get('Pipeline', 'dupfile_stem')
            recordfile_stem = parser.get('Pipeline', 'recordfile_stem')
            eventfile_stem = parser.get('Pipeline', 'eventfile_stem')

            return [serv_name, username, password, server_dir, scraper_stem,
                    dupfile_stem, recordfile_stem, eventfile_stem]
        except Exception, e:
            print 'There was an error. Check the log file for more information.'
            logger.warning('Problem parsing config file. {}'.format(e))
    else:
        cwd = os.path.abspath(os.path.dirname(__file__))
        config_file = os.path.join(cwd, 'default_config.ini')
        parser.read(config_file)
        logger.info('No config found. Using default.')
        try:
            collection = parser.get('Database', 'collection_list')
            whitelist = parser.get('URLS', 'file')
            return collection, whitelist
        except Exception, e:
            print 'There was an error. Check the log file for more information.'
            logger.warning('Problem parsing config file. {}'.format(e))


if __name__ == '__main__':

    logger = logging.getLogger('pipeline_log')
    logger.setLevel(logging.INFO)

    fh = logging.FileHandler('pipeline.log', 'w')
    formatter = logging.Formatter('%(levelname)s %(asctime)s: %(message)s')
    fh.setFormatter(formatter)

    logger.addHandler(fh)
    logger.info('Running')

    logger.info('PHOX.pipeline run')
    print '\nPHOX.pipeline run:', datetime.datetime.now()

    if len(sys.argv) > 1:
        datesuffix = sys.argv[1]
        logger.info('Date suffix: ' + datesuffix + '\n')
        print 'Date suffix:', datesuffix
    else:
        logger.info('Error: No date suffix in PHOX.pipeline')
        sys.exit()

    logger.info('Parsing config file.')
    config_options = parse_config()
    servername = config_options[0]
    username = config_options[1]
    password = config_options[2]
    serverdir = config_options[3]

    scraperstem = config_options[4]
    dupfilestem = config_options[5]
    recordfilestem = config_options[6]
    eventfilestem = config_options[7]

    # this is actually generated inside Mongo.formatter.py
    scraperfilename = scraper_connection.main()
    logger.info("Scraper file name: " + scraperfilename)
    print "Scraper file name:", scraperfilename

    logger.info("Running Mongo.formatter.py \n ")
    print "Running Mongo.formatter.py"
    mongo_formatter.main(datesuffix)

    recordfilename = recordfilestem + datesuffix + '.txt'
    logger.info("Record file name: " + recordfilename)
    print "Record file name:", recordfilename

    eventfilename = eventfilestem + datesuffix + '.txt'
    logger.info("Event file name: " + eventfilename)
    print "Event file name:", eventfilename

    logger.info("Running TABARI")
    print "Running TABARI"
    subprocess.call(
        "./TABARI.0.8.4b1 -ad PHOX.pipeline.project -t " +
        recordfilename +
        " -o " +
        eventfilename,
        shell=True)

    logger.info("Running python OneADay.reformatter.py " + eventfilename)
    print "Running python OneADay.reformatter.py", eventfilename
    oneaday_formatter.main(eventfilename)

    try:
        ftp = FTP(servername)     # connect to host, default port
        ftp.login(username, password)
        ftp.cwd(serverdir)               # change into PHOX directory
        logger.info('Logged into: ' + servername + '/' + serverdir)
        print 'Logged into:', servername, '/', serverdir
    except:
        logger.info('Login to ' + servername + ' unsuccessful')
        print 'Login to', servername, 'unsuccessful'
        sys.exit()

    try:
        eventfilename = eventfilestem + datesuffix + '.filtered.txt'
        ftp.storlines("STOR " + eventfilename, open(eventfilename))
        logger.info('Successfully transferred ' + eventfilename + '\n')
        print 'Successfully transferred', eventfilename
        eventfilezip = eventfilename + '.zip'
        subprocess.call("zip " + eventfilezip + ' ' + eventfilename,
                        shell=True)
        ftp.storbinary("STOR " + eventfilezip, open(eventfilezip))
    except:
        logger.info('Transfer of ' + eventfilename + ' unsuccessful')
        print 'Transfer of', eventfilename, 'unsuccessful'

    try:
        dupfilename = dupfilestem + datesuffix + '.txt'
        ftp.storlines("STOR " + dupfilename, open(dupfilename))
        logger.info('Successfully transferred ' + dupfilename)
        print 'Successfully transferred', dupfilename
    except:
        logger.info('Transfer of ' + dupfilename + ' unsuccessful')
        print 'Transfer of', dupfilename, 'unsuccessful'

    ftp.quit()
    logger.info('PHOX.pipeline end')
    print 'PHOX.pipeline end:', datetime.datetime.now()
