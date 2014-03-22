import re
import sys
import textwrap
import phox_utilities
from dateutil import parser


MAX_LEDE = 4   # number of sentences to output
# this is relatively high because we are only looking for sentences that
# will have subject and object
MIN_SENTLENGTH = 100
MAX_SENTLENGTH = 512

MAX_URLLENGTH = 192   # temporary to accommodate TABARI input limits

newsourcestem = 'newsources.'

sources = {
    'csmonitor.com': 'CSM',
    'bbc.co.uk': 'BBC',
    'reuters.com': 'REU',
    'xinhuanet.com': 'XIN',
    'www.upi.com': 'UPI',
    'nytimes.com': 'NYT',
    'todayszaman.com': 'TZA',
    'hosted2.ap.org': 'APP',
    'theguardian.com': 'GUA',
    'todayszaman.com': 'TZA',
    'insightcrime.org': 'ISC',
    'france24.com': 'FRA',
    'yahoo.com': 'YAH',
    'allafrica.com': 'ALA',
    'voanews.com': 'VOA',
    'aljazeera.com': 'AJZ',
    'AlAkhbarEnglish': 'AKB',
    'usatoday.com': 'USA',
    'latimes.com': 'LAT',
    'foxnews.com': 'FOX',
    'IRINnews.org': 'IRI',
    'rfi.fr': 'RFI',
    'cnn.com': 'CNN',
    'abcnews': 'ABC', 
    'wsj.com': 'WSJ', 
    'nydailynews.com': 'NYD', 
    'washingtonpost.com': 'WAS', 
    'chicagotribune.com': 'CHT',
    'miamiherald.com':'MIA',
    'thestar.com.my':'STA',
    'al-monitor.com':'MON',
    'un.org':'UNN',
    'pakistantelegraph.com':'PAK',
    'africaleader.com':'AFR',
    'theuknews.com':'UKN',
    'indiagazette.com':'ING',
    'mexicostar.com':'MES',
    'japanherald.com':'JAH',
    'chinanationalnews.com':'CHN',
    'jpost.com':'JPO',
    'middleeaststar.com':'MEA',
    'beijingbulletin.com':'BEB',
    'arabherald.com':'ARH',
    'russiaherald.com':'RUH',
    'iraqsun.com':'IRA',
    'malaysiasun.com':'MAL',
    'thehindu.com':'HIN',
    'shanghaisun.com':'SHA',
    'dawn.com':'DAW',
    'singaporestar.com':'SNG',
    'southeastasiapost.com':'SEA',
    'afghanistansun.com':'AFG',
    'australianherald.com':'AUS',
    'thenational.ae':'NAT',
    'europesun.com':'EUS',
    'greekherald.com':'GKH',
    'irishsun.com':'IRS',
    'iranherald.com':'IAH',
    'parisguardian.com':'PAG',
    'brazilsun.com':'BZS',
    'asiabulletin.com':'ASB',
    'nigeriasun.com':'NIG',
    'haitisun.com':'HAI',
    'philippinetimes.com':'PHI',
    'venezuelastar.com':'VEN',
    'caribbeanherald.com':'CAR',
    'zimbabwestar.com':'ZIM',
    'torontotelegraph.com':'TOR',
    'bangladeshsun.com':'BAN',
    'sydneysun.com':'SYD',
    'menafn.com':'MEN',
    'thelocal.se':'TLO',
    'nepalnational.com':'NEP',
    'kenyastar.com':'KEN',
    'sierraleonetimes.com':'SIE',
    'hurriyetdailynews.com':'HDN',
    'pajhwok.com':'PAJ',
    'thelocal.it':'LIT',
    'thelocal.fr':'LFR',
    'thelocal.es':'LES',
    'thelocal.ch':'LCH',
    'ipsnews.net':'IPS',
    'jamaicantimes.com':'JAM',
    'argentinastar.com':'ARG',
    'srilankasource.com':'SRI',
    'centralasiatimes.com':'CEN',
    'thelocal.no':'LNO',
    'theeastafrican.co.ke':'ESF',
    'taiwansun.com':'TAI',
    'cambodiantimes.com':'CAM',
    'payvand.com':'PAY',
    'trinidadtimes.com':'TRD',
    'yementimes.com':'YEM',
    'hongkongherald.com':'HNK'}

def get_date(field):
    """
    Function to extract date from story header in scraper_results.

    Parameters
    ------
    field: String
            Story header.

    Returns
    -------

    date : String
            Date string in the form YYMMDD
    """
    if 'csmonitor.com' in field[-1]:
        csmdate = field[-1].split('/20')[1].split('/')
        date = csmdate[0] + csmdate[1]
    elif field[1]:
        try:
            date_obj = parser.parse(field[1])
            date = str(date_obj)[2:4] + str(date_obj)[5:7] + str(date_obj)[8:10]
        except:
            date = '000000'
    else:
        date = '000000'

    return date


def write_record(source, sourcecount, thisdate, thisURL, story, fout):
    """
    Function to write TABARI-formatted story record.

    Parameters
    ------
    source: String
            Story news source.

    sourcecount: Dictionary
            Dictionary of counts of stories by source

    thisdate: String
            Date string in the form YYMMDD

    thisURL: String
            String of URL to story at source

    story: String
            News article story

    fout: file
            File for TABARI-formatted output
    """
    if source in sourcecount:  # count of the stories by source
        sourcecount[source] += 1
    else:
        sourcecount[source] = 1

    sentlist = sentence_segmenter(story)

    nsent = 1
    for sent in sentlist:
        if sent[0] != '"':  # skip sentences beginning with quotes
            print thisdate, source, thisURL
           # print >> fout, thisdate + ' ' + source + '-' + \
           #     str(sourcecount[source]).zfill(4) + '-' + \
           #     str(nsent) + ' ' + thisURL  # + '\n'
            print >> fout, thisdate + ' ' + thisURL
            lines = textwrap.wrap(sent, 80)
            for txt in lines:
                print >> fout, txt

            print >> fout, '\n'

        nsent += 1
        if nsent > MAX_LEDE:
            break


def sentence_segmenter(paragr):
    """ 
    Function to break a string 'paragraph' into a list of sentences based on the following rules:

    1. Look for terminal [.,?,!] followed by a space and [A-Z]
    2. If ., check against abbreviation list ABBREV_LIST: Get the string between the . and the
       previous blank, lower-case it, and see if it is in the list. Also check for single-
       letter initials. If true, continue search for terminal punctuation
    3. Extend selection to balance (...) and "...". Reapply termination rules
    4. Add to sentlist if the length of the string is between MIN_SENTLENGTH and MAX_SENTLENGTH
    5. Returns sentlist 

    Parameters
    ------
    paragr: String
            
    """
#   ka = 0
#   print '\nSentSeg-Mk1'
# sentence termination pattern used in sentence_segmenter(paragr)
    terpat = re.compile('[\.\?!]\s+[A-Z\"]')

    # source: LbjNerTagger1.11.release/Data/KnownLists/known_title.lst from
    # University of Illinois with editing
    ABBREV_LIST = [
        'mrs.', 'ms.', 'mr.', 'dr.', 'gov.', 'sr.', 'rev.', 'r.n.', 'pres.',
        'treas.', 'sect.', 'maj.', 'ph.d.', 'ed. psy.', 'proc.', 'fr.', 'asst.', 'p.f.c.', 'prof.',
        'admr.', 'engr.', 'mgr.', 'supt.', 'admin.', 'assoc.', 'voc.', 'hon.', 'm.d.', 'dpty.',
        'sec.', 'capt.', 'c.e.o.', 'c.f.o.', 'c.i.o.', 'c.o.o.', 'c.p.a.', 'c.n.a.', 'acct.',
        'llc.', 'inc.', 'dir.', 'esq.', 'lt.', 'd.d.', 'ed.', 'revd.', 'psy.d.', 'v.p.',
        'senr.', 'gen.', 'prov.', 'cmdr.', 'sgt.', 'sen.', 'col.', 'lieut.', 'cpl.', 'pfc.',
        'k.p.h.', 'cent.', 'deg.', 'doz.', 'Fahr.', 'Cel.', 'F.', 'C.', 'K.', 'ft.', 'fur.',
        'gal.', 'gr.', 'in.', 'kg.', 'km.', 'kw.', 'l.', 'lat.', 'lb.', 'lb per sq in.',
        'long.', 'mg.', 'mm.,, m.p.g.', 'm.p.h.', 'cc.', 'qr.', 'qt.', 'sq.', 't.', 'vol.',
        'w.', 'wt.']

    sentlist = []
    searchstart = 0  # controls skipping over non-terminal conditions
    terloc = terpat.search(paragr)
    while terloc:
#       print 'Mk2-0:', paragr[:terloc.start()+2]
        isok = True
        if paragr[terloc.start()] == '.':
            if (paragr[terloc.start() - 1].isupper() and
                    paragr[terloc.start() - 2] == ' '):
                        isok = False      # single initials
            else:
                # check abbreviations
                loc = paragr.rfind(' ', 0, terloc.start() - 1)
                if loc > 0:
#                   print 'SentSeg-Mk1: checking',paragr[loc+1:terloc.start()+1]
                    if paragr[loc + 1:terloc.start() + 1].lower() in ABBREV_LIST:
#                       print 'SentSeg-Mk2: found',paragr[loc+1:terloc.start()+1]
                        isok = False
        if paragr[:terloc.start()].count('(') != paragr[:terloc.start()].count(')'):
#           print 'SentSeg-Mk2: unbalanced ()'
            isok = False
        if paragr[:terloc.start()].count('"') % 2 != 0  :
#           print 'SentSeg-Mk2: unbalanced ""'
            isok = False
        if isok:
            if (len(paragr[:terloc.start()]) > MIN_SENTLENGTH and
                    len(paragr[:terloc.start()]) < MAX_SENTLENGTH):
                sentlist.append(paragr[:terloc.start() + 2])
#               print 'SentSeg-Mk3: added',paragr[:terloc.start()+2]
            paragr = paragr[terloc.end() - 1:]
            searchstart = 0
        else:
            searchstart = terloc.start() + 2

#       print 'SentSeg-Mk4:',paragr[:64]
#       print '            ',paragr[searchstart:searchstart+64]
        terloc = terpat.search(paragr, searchstart)
#       ka += 1
#       if ka > 16: sys.exit()

    # add final sentence
    if (len(paragr) > MIN_SENTLENGTH and len(paragr) < MAX_SENTLENGTH):
        sentlist.append(paragr)

    return sentlist


def get_source(field):
    """
    Function to extract source from URL in story header.

    Parameters
    ------
    field: String
            Story header.

    Returns
    -------

    source : String
            3 char source code from sources dictionary
    """
    source_url = field[-1].strip()
    sourced = source_url.split('http://')[-1]

    try:
        key = [i for i, s in enumerate(sources.keys()) if s in sourced]
        if key:
            source = sources[sources.keys()[key[0]]]
        else:
            source = '999'
    except AttributeError:
        source = '999'

    return source


def get_story(story_all):
    """
    Function to extract story text without date and source line.

    Parameters
    ------
    story_all: String
            All story text.

    Returns

    -------

    story : String
            3 char source code from sources dictionary
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
        story = story_all.split("\xe2\x80\x93 ", 1)[1]
    else:
        story = story_all

    return story


def main(thisday):
    """
    Main function to parse scraper_results to TABARI-formatted output.
    """
    server_list, file_list =  phox_utilities.parse_config('PHOX_config.ini')
    scraperfilename = file_list.scraper_stem + thisday + '.txt'
    print "Mongo: Scraper file name:", scraperfilename

    recordfilename = file_list.recordfile_stem + thisday + '.txt'
    print "Mongo: Record file name:", recordfilename

    newsourcefile = newsourcestem + thisday + '.txt'
    print "Mongo: New Sources file name:", newsourcefile

    try:
        fin = open(scraperfilename, 'r')
    except IOError:
        print scraperfilename
        phox_utilities.do_RuntimeError(
            'Could not find the scraper file for',
            thisday)

    finlist = fin.readlines()
    fout = open(recordfilename, 'w')
    newout = open(newsourcefile, 'w')
    sourcecount = {}

    #storyno = 1
    #csno = 1

    for line in range(0, len(finlist)):
        if '\thttp' in finlist[line]:
            field = finlist[line].split('\t')
            thisURL = field[2][:-1]
            # temporary to accommodate TABARI input limits
            thisURL = thisURL[:MAX_URLLENGTH]

            thisstory = get_story(finlist[line + 1])
            thisdate = get_date(field)
            thissource = get_source(field)

            if thissource == '999':
                # Adds sources not included in sources dictionary to
                # 'newsource_results_20..' file output
                print >> newout, thisURL

            write_record(
                thissource,
                sourcecount,
                thisdate,
                thisURL,
                thisstory,
                fout)

    fin.close()
    fout.close()
    print "Finished"

if __name__ == '__main__':
    if len(sys.argv) > 2:  # initializations for stand-alone tests
        phox_utilities.init_logger('test_pipeline.log')
        logger = phox_utilities.logger  # get a local copy for the pipeline
        # initialize the various phox_utilities globals
        phox_utilities.parse_config('test_config.ini')

    if len(sys.argv) > 1:
        thisday = sys.argv[1]
    else:
        phox_utilities.do_RuntimeError('No date suffix in Mongo.formatter.py')

    main(thisday)
