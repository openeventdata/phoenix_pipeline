import os
import sys
import glob
import logging
import datetime
import subprocess
import phox_uploader
import phox_utilities
import mongo_formatter
import oneaday_formatter
import scraper_connection
from ftplib import FTP
from ConfigParser import ConfigParser


if __name__ == '__main__':

    phox_utilities.init_logger('PHOX_pipeline.log')
    logger = phox_utilities.logger  # get a local copy for the pipeline

    # initialize the various phox_utilities globals
    phox_utilities.parse_config('PHOX_config.ini')

    print '\nPHOX.pipeline run:', datetime.datetime.now()

    if len(sys.argv) > 1:
        date_string = sys.argv[1]
        logger.info('Date string: ' + date_string + '\n')
        print 'Date string:', date_string
    else:
        logger.info('Error: No date string in PHOX.pipeline')
        sys.exit()

    # this is actually generated inside Mongo.formatter.py
    # also we could just shift and use the config.ini info to get this
    scraperfilename = scraper_connection.main()
    subprocess.call("cp " + scraperfilename + " " +
                    phox_utilities.Scraper_Stem + date_string + ".txt",
                    shell=True)
    logger.info("Scraper file name: " + scraperfilename)
    print "Scraper file name:", scraperfilename

    logger.info("Running Mongo.formatter.py \n ")
    print "Running Mongo.formatter.py"
    mongo_formatter.main(date_string)

    logger.info("Running TABARI")
    print "Running TABARI"
    subprocess.call(
        "./TABARI.0.8.4b1 -ad PHOX.pipeline.project -t "
        + phox_utilities.Recordfile_Stem + date_string +
        ".txt -o " +
        phox_utilities.Fullfile_Stem + date_string + ".txt",
        shell=True)

    logger.info("Running oneaday_formatter.py ")
    print "Running oneaday_formatter.py"
    oneaday_formatter.main(date_string)

    logger.info("Running phox_uploader.py ")
    print "Running phox_uploader.py"
    phox_uploader.main(date_string)

    logger.info('PHOX.pipeline end')
    print 'PHOX.pipeline end:', datetime.datetime.now()
