import sys
import logging
import datetime
import subprocess
import phox_uploader
import phox_utilities
import mongo_formatter
import oneaday_formatter
import scraper_connection


phox_utilities.init_logger('PHOX_pipeline.log')
# get a local copy for the pipeline
logger = logging.getLogger('pipeline_log')

# initialize the various phox_utilities globals
server_details, file_details = phox_utilities.parse_config('PHOX_config.ini')

print '\nPHOX.pipeline run:', datetime.datetime.now()

if len(sys.argv) > 1:
    date_string = sys.argv[1]
    logger.info('Date string: ' + date_string + '\n')
    print 'Date string:', date_string
else:
    now = datetime.datetime.utcnow()
    date_string = '{:02d}{:02d}{:02d}'.format(now.year, now.month, now.day)
    logger.info('Date string: ' + date_string + '\n')
    print 'Date string:', date_string

# this is actually generated inside Mongo.formatter.py
# also we could just shift and use the config.ini info to get this
scraperfilename = scraper_connection.main(file_details.scraper_stem)
logger.info("Scraper file name: " + scraperfilename)
print "Scraper file name:", scraperfilename

logger.info("Running Mongo.formatter.py \n ")
print "Running Mongo.formatter.py"
mongo_formatter.main(date_string)

logger.info("Running TABARI")
print "Running TABARI"
subprocess.call(
    "./TABARI.0.8.4b1 -ad PHOX.pipeline.project -t "
    + file_details.recordfile_stem + date_string +
    ".txt -o " +
    file_details.fullfile_stem + date_string + ".txt",
    shell=True)

logger.info("Running oneaday_formatter.py ")
print "Running oneaday_formatter.py"
oneaday_formatter.main(date_string)

logger.info("Running phox_uploader.py ")
print "Running phox_uploader.py"
phox_uploader.main(date_string)

logger.info('PHOX.pipeline end')
print 'PHOX.pipeline end:', datetime.datetime.utcnow()
