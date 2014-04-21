import nltk.data
import datetime
import logging
import codecs
from pymongo import MongoClient


def make_conn():
    """
    Function to establish a connection to a local MonoDB instance.

    Returns
    -------

    collection: pymongo.collection.Collection. Iterable.
                Collection within MongoDB that holds the scraped news stories.

    """
    client = MongoClient()
    database = client.event_scrape
    collection = database['stories']
    return collection


def query_all(collection, less_than_date, greater_than_date, write_file=False):
    """
    Function to query the MongoDB instance and obtain results for the desired
    date range.

    Parameters
    ----------

    collection: pymongo.collection.Collection. Iterable.
                Collection within MongoDB that holds the scraped news stories.

    less_than_date:

    Returns
    -------
    """
    output = []
    posts = collection.find({"$and": [{"date_added": {"$lte": less_than_date}},
                                      {"date_added": {"$gt":
                                                      greater_than_date}}
                                      ]})

    sent_detector = nltk.data.load('tokenizers/punkt/english.pickle')

    final_out = ''
    if write_file:
        for num, post in enumerate(posts):
            try:
                #print 'Processing entry {}...'.format(num)
                content = post['content'].encode('utf-8')
                if post['source'] == 'aljazeera':
                    content = content.replace("""Caution iconAttention The browser or device you are using is out of date.  It has known security flaws and a limited feature set.  You will not see all the features of some websites.  Please update your browser.""", '')
                header = '  '.join(sent_detector.tokenize(content.strip())[:4])
                string = '{}\t{}\t{}\n{}\n'.format(num, post['date'],
                                                   post['url'], header)
                output.append(string)
            except Exception as e:
                print 'Error on entry {}: {}.'.format(num, e)
        final_out = '\n'.join(output)
        posts = collection.find({"$and": [{"date_added": {"$lte":
                                                          less_than_date}},
                                          {"date_added": {"$gt":
                                                          greater_than_date}}]}
                                )

    return posts, final_out


def main(current_date, write_file=False, file_stem=None):
    """

    Parameters
    ----------

    current_date: datetime object.
                    Date for which records are pulled. Normally this is
                    $date_running - 1. For example, if the script is running on
                    the 25th, the current_date will be the 24th.

    write_file: Boolean.
                Option indicating whether to write the results from the web
                scraper to an intermediate file. Defaults to false.

    file_stem: String. Optional.
                Optional string defining the file stem for the intermediate
                file for the scraper results.

    """
    conn = make_conn()

    less_than = datetime.datetime(current_date.year, current_date.month,
                                  current_date.day)
    less_than = less_than + datetime.timedelta(days=1)
    greater_than = less_than - datetime.timedelta(days=1)

    results, text = query_all(conn, less_than, greater_than,
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
            print 'Need filestem to write results to file.'

    return results, filename

if __name__ == '__main__':
    print 'Running...'
    main('temp_stem.', 'YYMMDD')
