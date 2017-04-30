from __future__ import print_function
from __future__ import unicode_literals
import utilities
import datetime
import logging
import codecs


def query_all(collection, lt_date, gt_date, sources, write_file=False):
    """
    Function to query the MongoDB instance and obtain results for the desired
    date range. The query constructed is: greater_than_date > results
    < less_than_date.

    Parameters
    ----------

    collection: pymongo.collection.Collection.
                Collection within MongoDB that holds the scraped news stories.

    lt_date: Datetime object.
                    Date for which results should be older than. For example,
                    if the date running is the 25th, and the desired date is
                    the 24th, then the `lt_date` is the 25th.

    gt_date: Datetime object.
                        Date for which results should be older than. For
                        example, if the date running is the 25th, and the
                        desired date is the 24th, then the `gt_date`
                        is the 23rd.

    sources: List.
                Sources to pull from the MongoDB instance.

    write_file: Boolean.
                Option indicating whether to write the results from the web
                scraper to an intermediate file. Defaults to false.

    Returns
    -------

    posts: List.
            List of dictionaries of results from the MongoDB query.


    final_out: String.
                If `write_file` is True, this contains a string representation
                of the query results. Otherwise, contains an empty string.

    """

    logger = logging.getLogger('pipeline_log')
    final_out = ''
    if write_file:
        output = []
        sent_detector = nltk.data.load('tokenizers/punkt/english.pickle')
        posts = collection.find({"$and": [{"date_added": {"$lt": lt_date}},
                                          {"date_added": {"$gt": gt_date}},
                                          {"source": {"$in": sources}}]})
        for num, post in enumerate(posts):
            try:
                #print 'Processing entry {}...'.format(num)
                content = post['content'].encode('utf-8')
                if post['source'] == 'aljazeera':
                    content = content.replace("""Caution iconAttention The browser or device you are using is out of date.  It has known security flaws and a limited feature set.  You will not see all the features of some websites.  Please update your browser.""", '')
                header = '  '.join(utilities.sentence_segmenter(content)[:4])
                string = '{}\t{}\t{}\n{}\n'.format(num, post['date'],
                                                   post['url'], header)
                output.append(string)
            except Exception as e:
                print('Error on entry {}: {}.'.format(num, e))
        final_out = '\n'.join(output)

    posts = collection.find({"$and": [{"date_added": {"$lte": lt_date}},
                                      {"date_added": {"$gt": gt_date}},
                                      {"source": {"$in": sources}}]})
    #posts = collection.find()

    print('Total number of stories: {}'.format(posts.count()))
    logger.info('Total number of stories: {}'.format(posts.count()))

    posts = list(posts)

    return posts, final_out


def _get_sources(filepath):
    """
    Function to create a list of sources that will be used in the query to the
    MongoDB instance.

    Parameters
    ----------

    filepath: String.
                Path to file containing the source keys. File should have a
                single key on each line.


    Returns
    -------

    sources: List.
                List of sources, basically.

    """
    with open(filepath, 'r') as f:
        sources = [key.replace('\n', '') for key in f.readlines()]
    return sources


def main(current_date, file_details, write_file=False, file_stem=None):
    """
    Function to create a connection to a MongoDB instance, query for a given
    day's results, optionally write the results to a file, and return the
    results.

    Parameters
    ----------

    current_date: datetime object.
                    Date for which records are pulled. Normally this is
                    $date_running - 1. For example, if the script is running on
                    the 25th, the current_date will be the 24th.

    file_details: Named tuple.
                    Tuple containing config information.

    write_file: Boolean.
                Option indicating whether to write the results from the web
                scraper to an intermediate file. Defaults to false.

    file_stem: String. Optional.
                Optional string defining the file stem for the intermediate
                file for the scraper results.

    Returns
    -------

    posts: Dictionary.
            Dictionary of results from the MongoDB query.

    filename: String.
                If `write_file` is True, contains the filename to which the
                scraper results are writen. Otherwise is an empty string.

    """
    sources = _get_sources('source_keys.txt')
    conn = utilities.make_conn(file_details.db_db, file_details.db_collection,
                               file_details.auth_db, file_details.auth_user,
                               file_details.auth_pass, file_details.db_host)

    less_than = datetime.datetime(current_date.year, current_date.month,
                                  current_date.day)
    greater_than = less_than - datetime.timedelta(days=1)
    less_than = less_than + datetime.timedelta(days=1)

    results, text = query_all(conn, less_than, greater_than, sources,
                              write_file=write_file)

    filename = ''
    if text:
        text = text.decode('utf-8')

        if file_stem:
            filename = '{}{:02d}{:02d}{:02d}.txt'.format(file_stem,
                                                         current_date.year,
                                                         current_date.month,
                                                         current_date.day)
            with codecs.open(filename, 'w', encoding='utf-8') as f:
                f.write(text)
        else:
            print('Need filestem to write results to file.')

    return results, filename
