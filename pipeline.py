import sys
import logging
import datetime
import uploader
import utilities
import formatter
import oneaday_formatter
import scraper_connection
from petrarch import petrarch


def main(write_petr=False, oneaday_filter=True):
    utilities.init_logger('PHOX_pipeline.log')
    # get a local copy for the pipeline
    logger = logging.getLogger('pipeline_log')

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
    formatted = formatter.main(results, file_details,
                               process_date, date_string)

    logger.info("Running PETRARCH")
    print "Running PETRARCH"
    file_details.fullfile_stem + date_string
    if write_petr and not oneaday_filter:
        petrarch.run_pipeline(formatted,
                              '{}{}.txt'.format(file_details.fullfile_stem,
                                                date_string))
        results = ''
    elif write_petr and oneaday_filter:
        print "Can't write to output and run the one-a-day filter. Exiting."
        logger.warning("Can't write to output and run the one-a-day filter. Exiting.")
        sys.exit()
    elif not write_petr and oneaday_filter:
        results = petrarch.run_pipeline(formatted, write_output=False)
    else:
        print "Can't run with the options you've specified. You need to fix something."
        logger.warning("Can't run with the options you've specified. Exiting.")
        sys.exit()

    if oneaday_filter:
        logger.info("Running oneaday_formatter.py")
        print "Running oneaday_formatter.py"
        oneaday_formatter.main(date_string, server_details, file_details)

    logger.info("Running phox_uploader.py")
    print "Running phox_uploader.py"
    uploader.main(results, date_string, server_details, file_details)

    logger.info('PHOX.pipeline end')
    print 'PHOX.pipeline end:', datetime.datetime.utcnow()


if __name__ == '__main__':
    # initialize the various utilities globals
    server_details, file_details = utilities.parse_config('PHOX_config.ini')

    main(file_details.petr_write, file_details.oneaday_filter)
