from __future__ import print_function
from __future__ import unicode_literals
import sys
import logging
import datetime
import uploader
import utilities
import formatter
import postprocess
import oneaday_filter
import result_formatter
import scraper_connection
from petrarch import petrarch


def main(file_details, server_details, logger_file=None, run_filter=None):
    """
    Main function to run all the things.

    Parameters
    ----------

    file_details: Named tuple.
                    All the other config information not in ``server_details``.

    server_details: Named tuple.
                    Config information specifically related to the remote
                    server for FTP uploading.

    logger_file: String.
                    Path to a log file. Defaults to ``None`` and opens a
                    ``PHOX_pipeline.log`` file in the current working
                    directory.

    run_filter: String.
                Whether to run the ``oneaday_formatter``. Takes True or False
                (strings) as values.
    """
    if logger_file:
        utilities.init_logger(logger_file)
    else:
        utilities.init_logger('PHOX_pipeline.log')
    # get a local copy for the pipeline
    logger = logging.getLogger('pipeline_log')

    print('\nPHOX.pipeline run:', datetime.datetime.utcnow())

    if len(sys.argv) > 1:
        date_string = sys.argv[1]
        process_date = datetime.datetime.utcnow() - datetime.timedelta(days=1)
        logger.info('Date string: {}'.format(date_string))
        print('Date string:', date_string)
    else:
        process_date = datetime.datetime.utcnow() - datetime.timedelta(days=1)
        date_string = '{:02d}{:02d}{:02d}'.format(process_date.year,
                                                  process_date.month,
                                                  process_date.day)
        logger.info('Date string: {}'.format(date_string))
        print('Date string:', date_string)

    results, scraperfilename = scraper_connection.main(process_date,
                                                       file_details)

    if scraperfilename:
        logger.info("Scraper file name: " + scraperfilename)
        print("Scraper file name:", scraperfilename)

    logger.info("Running Mongo.formatter.py")
    print("Running Mongo.formatter.py")
    formatted = formatter.main(results, file_details,
                               process_date, date_string)

    logger.info("Running PETRARCH")
    file_details.fullfile_stem + date_string
    if run_filter == 'False':
        print('Running PETRARCH and writing to a file. No one-a-day.')
        logger.info('Running PETRARCH and writing to a file. No one-a-day.')
        #Command to write output to a file directly from PETR
#        petrarch.run_pipeline(formatted,
#                              '{}{}.txt'.format(file_details.fullfile_stem,
#                                                date_string), parsed=True)
        petr_results = petrarch.run_pipeline(formatted, write_output=False,
                                             parsed=True)
    elif run_filter == 'True':
        print('Running PETRARCH and returning output.')
        logger.info('Running PETRARCH and returning output.')
        petr_results = petrarch.run_pipeline(formatted, write_output=False,
                                             parsed=True)
    else:
        print("""Can't run with the options you've specified. You need to fix
              something.""")
        logger.warning("Can't run with the options you've specified. Exiting.")
        sys.exit()

    if run_filter == 'True':
        logger.info("Running oneaday_formatter.py")
        print("Running oneaday_formatter.py")
        formatted_results = oneaday_filter.main(petr_results)
    else:
        logger.info("Running result_formatter.py")
        print("Running result_formatter.py")
        formatted_results = result_formatter.main(petr_results)

    logger.info("Running postprocess.py")
    print("Running postprocess.py")
    postprocess.main(formatted_results, date_string, file_details)

    logger.info("Running phox_uploader.py")
    print("Running phox_uploader.py")
    uploader.main(date_string, server_details, file_details)

    logger.info('PHOX.pipeline end')
    print('PHOX.pipeline end:', datetime.datetime.utcnow())


if __name__ == '__main__':
    # initialize the various utilities globals
    server_details, file_details = utilities.parse_config('PHOX_config.ini')

    main(file_details, server_details, file_details.log_file,
         run_filter=file_details.oneaday_filter)
