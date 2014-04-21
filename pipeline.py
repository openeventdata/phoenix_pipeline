import sys
import logging
import datetime
import uploader
import utilities
import formatter
import subprocess
import oneaday_formatter
import scraper_connection


utilities.init_logger('PHOX_pipeline.log')
# get a local copy for the pipeline
logger = logging.getLogger('pipeline_log')

# initialize the various utilities globals
server_details, file_details = utilities.parse_config('PHOX_config.ini')

print '\nPHOX.pipeline run:', datetime.datetime.utcnow()

if len(sys.argv) > 1:
    date_string = sys.argv[1]
    process_date = datetime.datetime.utcnow() - datetime.timedelta(days=1)
    logger.info('Date string: {}'.format(date_string))
    print 'Date string:', date_string
else:
    process_date = datetime.datetime.utcnow() - datetime.timedelta(days=1)
    date_string = '{:02d}{:02d}{:02d}'.format(process_date.year,
                                              process_date.month,
                                              process_date.day)
    logger.info('Date string: {}'.format(date_string))
    print 'Date string:', date_string

results, scraperfilename = scraper_connection.main(process_date)

if scraperfilename:
    logger.info("Scraper file name: " + scraperfilename)
    print "Scraper file name:", scraperfilename

logger.info("Running Mongo.formatter.py")
print "Running Mongo.formatter.py"
formatter.main(results, server_details, file_details, process_date,
               date_string)

logger.info("Running TABARI")
print "Running TABARI"
subprocess.call(
    "./TABARI.0.8.4b2 -ad PHOX.pipeline.project -t "
    + file_details.recordfile_stem + date_string +
    ".txt -o " +
    file_details.fullfile_stem + date_string + ".txt",
    shell=True)

logger.info("Running oneaday_formatter.py")
print "Running oneaday_formatter.py"
oneaday_formatter.main(date_string, server_details, file_details)

logger.info("Running phox_uploader.py")
print "Running phox_uploader.py"
uploader.main(date_string, server_details, file_details)

logger.info('PHOX.pipeline end')
print 'PHOX.pipeline end:', datetime.datetime.utcnow()
