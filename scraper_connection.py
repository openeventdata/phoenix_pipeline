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
    posts = collection.find({"$and": [{"date_added": {"$lt": less_than_date}},
                                      {"date_added": {"$gt": greater_than_date}}
                                      ]})

    sent_detector = nltk.data.load('tokenizers/punkt/english.pickle')

    for num, post in enumerate(posts):
        try:
            print 'Processing entry {}...'.format(num)
            content = post['content'].encode('utf-8')
            if post['source'] == 'aljazeera':
                content = content.replace("""Caution iconAttention The browser or device you are using is out of date.  It has known security flaws and a limited feature set.  You will not see all the features of some websites.  Please update your browser.""", '')
            header = '  '.join(sent_detector.tokenize(content.strip())[:4])
            string = '{}\t{}\t{}\n{}\n'.format(num, post['date'], post['url'],
                                               header)
            output.append(string)
        except Exception:
            print 'Error on entry {}...'.format(num)
    final_out = '\n'.join(output)
    return final_out


def main():
    conn = make_conn()

    curr = datetime.datetime.utcnow()
    less_than = datetime.datetime(curr.year, curr.month, curr.day)
    greater_than = less_than - datetime.timedelta(days=2)
    desired_date = less_than - datetime.timedelta(days=1)

    text = query_all(conn, less_than, greater_than)
    text = text.decode('utf-8')

    filename = 'scraper_results_{:02d}{:02d}{:02d}.txt'.format(desired_date.year,
                                                               desired_date.month,
                                                               desired_date.day)
    with codecs.open(filename, 'w', encoding='utf-8') as f:
        f.write(text)

    return filename

if __name__ == '__main__':
    print 'Running...'

    logging.basicConfig()

    sched = Scheduler()
    sched.add_cron_job(main, hour='6')

    sched.start()
    while True:
        time.sleep(10)
    sched.shutdown()
