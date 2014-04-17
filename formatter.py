import re
import sys
import timex
import textwrap
import datetime
import utilities
from dateutil import parser
from collections import Counter


def format_records(results, max_lede, process_date):
    """

    Parameters
    -----

    results: pymongo.cursor.Cursor. Iterable.
            Iterable containing the results from the scraper.

    max_lede: Integer.
                How many sentences to include from the news story.

    process_date: datetime object.
                    Datetime object indicating which date the pipeline is
                    processing. Standard is date_running - 1 day.

    Returns
    ------

    final_output: String.
                    TABARI-formatted records with one sentence per entry.

    sourcecount: collections.Counter object.
                    Dictionary of sources with the source (as defined within
                    scraper whitelist_urls.csv file) as the key and the count
                    of how many stories came from that source as the value.

    """
    sourcecount = Counter()

    max_lede = 4
    final_output = ''
    for story in results:
        try:
            content = story['content'].encode('utf-8')
        except Exception as e:
            print 'Error encoding text for {}. {}'.format(story['url'], e)
        final_content = _format_content(content)

        MAX_URLLENGTH = 192   # temporary to accommodate TABARI input limits
        url = story['url'][:MAX_URLLENGTH]

        date = _get_date(story, process_date)

        source = story['source']
        sourcecount[source] += 1

        #Filter sentences that start with a "
        final_content = [sent for sent in final_content if sent[0] != '"']
        for sent in final_content[:max_lede]:
            try:
                sent = sent.encode('utf-8')
            except Exception as e:
                print 'Error with Unicode on {}. {}'.format(url, e)
            line = '\n'.join(textwrap.wrap(sent, 80))
            line = line.decode('utf-8')
            line = line.encode('utf-8')
            try:
                url = url.encode('utf-8')
            except UnicodeDecodeError:
                url = url.decode('utf-8')
            try:
                one_record = '{} {}\n{}\n\n'.format(date, url, line)
            except UnicodeEncodeError:
                print 'Error on: {}'.format(url, line)

            final_output += one_record

    return final_output, sourcecount


def _format_content(raw_content):
    """
    Function to process a given news story for further formatting.

    Parameters
    ------

    raw_content: String.
                    Content of a news story as pulled from the web scraping
                    database.

    Returns
    --------

    sent_list: List.
                List of sentences.

    """
    content = _get_story(raw_content)
    sent_list = _sentence_segmenter(content)
    return sent_list


def _get_story(story_all):
    """
    Function to extract story text without date and source line.

    Parameters
    ------

    story_all: String.
                Content of a news story as pulled from the web scraping
                database.

    Returns
    -------

    story: String.
            Content of story with header/frontmatter removed.

    """

    if '(Reuters)' in story_all:
        story = story_all[story_all.find('(Reuters)') + 12:]
    elif '(IANS)' in story_all:
        story = story_all[story_all.find('(IANS)') + 7:]
    elif '(ANI)' in story_all:
        story = story_all[story_all.find('(ANI)') + 7:]
    elif '(Xinhua) -- ' in story_all:
        story = story_all[story_all.find('(Xinhua) -- ') + 12:]
    elif '(UPI) -- ' in story_all:
        story = story_all[story_all.find('(UPI) -- ') + 9:]
    if bool(re.search("\xe2\x80\x93", story_all[0:32])):
        try:
            story = story_all.split("\xe2\x80\x93 ", 1)[1]
        except IndexError:
            story = story_all
    else:
        story = story_all

    return story


def _sentence_segmenter(paragr):
    """
    Function to break a string 'paragraph' into a list of sentences based on
    the following rules:

    1. Look for terminal [.,?,!] followed by a space and [A-Z]
    2. If ., check against abbreviation list ABBREV_LIST: Get the string
    between the . and the previous blank, lower-case it, and see if it is in
    the list. Also check for single-letter initials. If true, continue search
    for terminal punctuation
    3. Extend selection to balance (...) and "...". Reapply termination rules
    4. Add to sentlist if the length of the string is between MIN_SENTLENGTH
    and MAX_SENTLENGTH
    5. Returns sentlist

    Parameters
    ------

    paragr: String.
            Content that will be split into constituent sentences.

    Returns
    --------

    sentlist: List.
                List of sentences.

    """
    # this is relatively high because we are only looking for sentences that
    # will have subject and object
    MIN_SENTLENGTH = 100
    MAX_SENTLENGTH = 512

    # sentence termination pattern used in sentence_segmenter(paragr)
    terpat = re.compile('[\.\?!]\s+[A-Z\"]')

    # source: LbjNerTagger1.11.release/Data/KnownLists/known_title.lst from
    # University of Illinois with editing
    ABBREV_LIST = ['mrs.', 'ms.', 'mr.', 'dr.', 'gov.', 'sr.', 'rev.', 'r.n.',
                   'pres.', 'treas.', 'sect.', 'maj.', 'ph.d.', 'ed. psy.',
                   'proc.', 'fr.', 'asst.', 'p.f.c.', 'prof.', 'admr.',
                   'engr.', 'mgr.', 'supt.', 'admin.', 'assoc.', 'voc.',
                   'hon.', 'm.d.', 'dpty.',  'sec.', 'capt.', 'c.e.o.',
                   'c.f.o.', 'c.i.o.', 'c.o.o.', 'c.p.a.', 'c.n.a.', 'acct.',
                   'llc.', 'inc.', 'dir.', 'esq.', 'lt.', 'd.d.', 'ed.',
                   'revd.', 'psy.d.', 'v.p.',  'senr.', 'gen.', 'prov.',
                   'cmdr.', 'sgt.', 'sen.', 'col.', 'lieut.', 'cpl.', 'pfc.',
                   'k.p.h.', 'cent.', 'deg.', 'doz.', 'Fahr.', 'Cel.', 'F.',
                   'C.', 'K.', 'ft.', 'fur.',  'gal.', 'gr.', 'in.', 'kg.',
                   'km.', 'kw.', 'l.', 'lat.', 'lb.', 'lb per sq in.', 'long.',
                   'mg.', 'mm.,, m.p.g.', 'm.p.h.', 'cc.', 'qr.', 'qt.', 'sq.',
                   't.', 'vol.',  'w.', 'wt.']

    sentlist = []
    # controls skipping over non-terminal conditions
    searchstart = 0
    terloc = terpat.search(paragr)
    while terloc:
        isok = True
        if paragr[terloc.start()] == '.':
            if (paragr[terloc.start() - 1].isupper() and
                    paragr[terloc.start() - 2] == ' '):
                        isok = False      # single initials
            else:
                # check abbreviations
                loc = paragr.rfind(' ', 0, terloc.start() - 1)
                if loc > 0:
                    if paragr[loc + 1:terloc.start() + 1].lower() in ABBREV_LIST:
                        isok = False
        if paragr[:terloc.start()].count('(') != paragr[:terloc.start()].count(')'):
            isok = False
        if paragr[:terloc.start()].count('"') % 2 != 0:
            isok = False
        if isok:
            if (len(paragr[:terloc.start()]) > MIN_SENTLENGTH and
                    len(paragr[:terloc.start()]) < MAX_SENTLENGTH):
                sentlist.append(paragr[:terloc.start() + 2])
            paragr = paragr[terloc.end() - 1:]
            searchstart = 0
        else:
            searchstart = terloc.start() + 2

        terloc = terpat.search(paragr, searchstart)

    # add final sentence
    if (len(paragr) > MIN_SENTLENGTH and len(paragr) < MAX_SENTLENGTH):
        sentlist.append(paragr)

    return sentlist


#The only one where we don't have a firm, consistent source in the
#story['source'] is the google news stuff.
#Deprecated for now since I don't see a clear purpose for it
#def _get_source(field):
#    """
#    Function to extract source from URL in story header.
#
#    Parameters
#    ------
#    field: String
#            Story header.
#
#    Returns
#    -------
#
#    source : String
#            3 char source code from sources dictionary
#    """
#    source_url = field[-1].strip()
#    sourced = source_url.split('http://')[-1]
#
#    try:
#        key = [i for i, s in enumerate(sources.keys()) if s in sourced]
#        if key:
#            source = sources[sources.keys()[key[0]]]
#        else:
#            source = '999'
#    except AttributeError:
#        source = '999'
#
#    return source


def _get_date(result_entry, process_date):
    """
    Function to extract date from a story. First checks for a date from the RSS
    feed itself. Then tries to pull a date from the first two sentences of a
    story. Finally turns to the date that the story was added to the database.
    For the dates pulled from the story, the function checks whether the
    difference is greater than one day from the date that the pipeline is
    parsing.

    Parameters
    ------

    result_entry: Dictionary.
                    Record of a single result from the web scraper.

    process_date: datetime object.
                    Datetime object indicating which date the pipeline is
                    processing. Standard is date_running - 1 day.


    Returns
    -------

    date : String.
            Date string in the form YYMMDD.

    """
    date_obj = ''
    if result_entry['date']:
        try:
            date_obj = parser.parse(result_entry['date'])
        except TypeError:
            date_obj = ''
    else:
        date_obj = ''

    if not date_obj:
        tagged = timex.tag(result_entry['content'][:2])
        dates = re.findall(r'<TIMEX2>(.*?)</TIMEX2>', tagged)
        if dates:
            try:
                date_obj = parser.parse(dates[0])
                diff_check = _check_date(date_obj, process_date)
                if diff_check:
                    date_obj = ''
            except TypeError:
                date_obj = ''
        else:
            date_obj = ''

    if not date_obj:
        date_obj = result_entry['date_added']

    date = '{}{:02d}{:02d}'.format(str(date_obj.year)[2:], date_obj.month,
                                   date_obj.day)

    return date


def _check_date(date_object, process_date):
    """
    Function to check the gap between the parsed date and the actual date that
    the pipeline is processing.

    Parameters
    --------

    date_object: datetime object.
                    Date that the _get_date function suggests as a candidate
                    date.

    process_date: datetime object.
                    Datetime object indicating which date the pipeline is
                    processing. Standard is date_running - 1 day.

    Returns
    ------

    too_big: Boolean.
                Whether the gap is one day or larger.

    """
    diff = date_object - process_date
    too_big = diff > datetime.timedelta(days=0)

    return too_big


def main(results, server_list, file_details, process_date, thisday):
    """
    Main function to parse results from the web scraper to TABARI-formatted
    output.
    """
    recordfilename = file_details.recordfile_stem + thisday + '.txt'
    print "Mongo: Record file name:", recordfilename

    #Suppress this for now.
#    newsourcefile = newsourcestem + thisday + '.txt'
#    print "Mongo: New Sources file name:", newsourcefile

    MAX_LEDE = 4
    results_to_write, source_counts = format_records(results, MAX_LEDE,
                                                     process_date)

    source_counts_string = ''
    for source, count in source_counts.iteritems():
        source_counts_string += '{}\t{}'.format(source, count)

#    with open(newsourcefile, 'w') as sauce:
#        sauce.write(source_counts_string)

    with open(recordfilename, 'w') as f:
        f.write(results_to_write)

    print "Finished"

if __name__ == '__main__':
    if len(sys.argv) > 2:  # initializations for stand-alone tests
        utilities.init_logger('test_pipeline.log')
        logger = utilities.logger  # get a local copy for the pipeline
        # initialize the various utilities globals
        utilities.parse_config('test_config.ini')

    if len(sys.argv) > 1:
        thisday = sys.argv[1]
    else:
        utilities.do_RuntimeError('No date suffix in Mongo.formatter.py')

    main(thisday)
