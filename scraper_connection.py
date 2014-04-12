import nltk.data
import datetime
import logging
import codecs
import time
from pymongo import MongoClient
from apscheduler.scheduler import Scheduler


def make_conn():
    client = MongoClient()
    database = client.event_scrape
    return database['stories']


def query_all(collection, less_than_date, greater_than_date):
    output = []
    posts = collection.find({"$and": [{"date_added": {"$lte": less_than_date}},
                                      {"date_added": {"$gt": greater_than_date}}
                                      ]})

    sent_detector = nltk.data.load('tokenizers/punkt/english.pickle')

    for num, post in enumerate(posts):
        try:
            #print 'Processing entry {}...'.format(num)
            content = post['content'].encode('utf-8')
            if post['source'] == 'aljazeera':
                content = content.replace("""Caution iconAttention The browser or device you are using is out of date.  It has known security flaws and a limited feature set.  You will not see all the features of some websites.  Please update your browser.""", '')
            header = '  '.join(sent_detector.tokenize(content.strip())[:4])
            string = '{}\t{}\t{}\n{}\n'.format(num, post['date'], post['url'],
                                               header)
            output.append(string)
        except Exception as e:
            print 'Error on entry {}: {}.'.format(num, e)
    final_out = '\n'.join(output)

    return final_out, posts


def main(file_stem, current_date):
    """
    Current date is date_running - 1 day. So if it's running on the 25th the
    date is the 24th.
    """
    conn = make_conn()

    less_than = datetime.datetime(current_date.year, current_date.month,
                                  current_date.day)
    less_than = less_than + datetime.timedelta(days=1)
    greater_than = less_than - datetime.timedelta(days=1)

    text, results = query_all(conn, less_than, greater_than)
    text = text.decode('utf-8')

    filename = '{}{:02d}{:02d}{:02d}.txt'.format(file_stem,
                                                 current_date.year,
                                                 current_date.month,
                                                 current_date.day)
    with codecs.open(filename, 'w', encoding='utf-8') as f:
        f.write(text)

    return filename, results

if __name__ == '__main__':
    print 'Running...'

    logging.basicConfig()

    sched = Scheduler()
    sched.add_cron_job(main, hour='6')

    sched.start()
    while True:
        time.sleep(10)
    sched.shutdown()
