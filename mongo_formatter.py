# coding: utf-8

# mongo_formatter.py
##
# This converts the first MAXLEDE sentences of scraper_results_* files (which in turn were 
# produced from a Mongo DB, hence the name of the program) into TABARI-formatted records.
# The header has the format 
#    YYMMDD AAA-MMMM-NN URL
#  where
#    YYMMDD: date
#    AAA:    source abbreviation
#    MMMM:   zero-filled sequence of the story within the source-day
#    N:     sentence number within the story
#    URL:    source story URL
#  Lines are wrapped to 80 characters 
##
# TO RUN PROGRAM:
##
# python mongo_formatter.py MMDD
#
# where MMDD is the month and day that will be appended to scraper_stem
##
# INPUT FILES: scraperstem+MMDD+.txt  (e.g. scraper_results_20140215.txt)
##
# OUTPUT FILES: recordfilestem+MMDD+.txt (e.g. eventrecords.1402015.txt
##
# PROGRAMMING NOTES: 
#
#  1. See notes in sentence_segmenter() on the conditions for segmenting sentences. The
#     list of abbreviations can be extended in the global ABBREV_LIST. The nltk segmenter
#     punkt could also be substituted here.
#
##
# SYSTEM REQUIREMENTS
# This program has been successfully run under Mac OS 10.6; it is standard Python 2.5
# so it should also run in Unix or Windows.
##
#	PROVENANCE:
#	Programmer: Philip A. Schrodt
#				Parus Analytical Systems
#				schrodt735@gmail.com
#				http://eventdata.parsuanalytics.edu
#
# Copyright (c) 2014	Philip A. Schrodt.	All rights reserved.
##
# This project was funded in part by National Science Foundation grant
# SES-1004414
##
# Redistribution and use in source and binary forms, with or without modification,
# are permitted under the terms of the MIT License: http://opensource.org/licenses/MIT
##
# Report bugs to: schrodt735@gmail.com
##
# REVISION HISTORY:
# 01-Feb-14:	Initial version
# 17-Feb-14:	Revised sentence segmenter
##
# ------------------------------------------------------------------------

import sys
import time
import textwrap
import re

# ======== global initializations ========= #


MAX_LEDE = 4   # number of sentences to output
MIN_SENTLENGTH = 100   # this is relatively high because we are only looking for sentences that will have subject and object
MAX_SENTLENGTH = 512

terpat = re.compile('[\.\?!]\s+[A-Z\"]')	# sentence termination pattern used in sentence_segmenter(paragr)

#source: LbjNerTagger1.11.release/Data/KnownLists/known_title.lst from University of Illinois with editing
ABBREV_LIST = ['mrs.', 'ms.', 'mr.', 'dr.', 'gov.', 'sr.', 'rev.',  'r.n.', 'pres.', 
	'treas.', 'sect.', 'maj.', 'ph.d.', 'ed. psy.', 'proc.', 'fr.', 'asst.', 'p.f.c.', 'prof.', 
	'admr.', 'engr.', 'mgr.', 'supt.', 'admin.', 'assoc.', 'voc.', 'hon.', 'm.d.', 'dpty.', 
	'sec.', 'capt.', 'c.e.o.', 'c.f.o.', 'c.i.o.', 'c.o.o.', 'c.p.a.', 'c.n.a.', 'acct.', 
	'llc.', 'inc.', 'dir.', 'esq.', 'lt.', 'd.d.', 'ed.', 'revd.', 'psy.d.', 'v.p.', 
	'senr.', 'gen.', 'prov.', 'cmdr.', 'sgt.', 'sen.', 'col.', 'lieut.', 'cpl.', 'pfc.', 
	'k.p.h.', 'cent.', 'deg.', 'doz.', 'Fahr.', 'Cel.', 'F.', 'C.', 'K.', 'ft.', 'fur.', 
	'gal.', 'gr.', 'in.', 'kg.', 'km.', 'kw.', 'l.', 'lat.', 'lb.', 'lb per sq in.', 
	'long.', 'mg.', 'mm.,, m.p.g.', 'm.p.h.', 'cc.', 'qr.', 'qt.', 'sq.', 't.', 'vol.', 
	'w.', 'wt.']

scraperstem = 'scraper_results_20'
recordfilestem = 'eventrecords.'

# ======== functions ========= #


def get_plaindate():  # processes date of the form 2014-01-28
    global field
    return field[1][2:4] + field[1][5:7] + field[1][8:10]

# processes date of the form 	Tue, 28 Jan 2014 23:40:04 +0530


def get_fmtdate():
    global field, source
    tifld = field[1].split()  # convert the date to YYMMDD
    tistr = ' '.join(tifld[1:4])
#		print tistr
    try:
        strctim = time.strptime(tistr, '%d %b %Y')
        return time.strftime('%y%m%d', strctim)
    except ValueError:
        print 'Cannot interpret date', field[1], 'in', thisURL
        source = ''
        return '000000'


def write_record():
    global thisdate, thisURL, source, story, fout, sourcecount

    if source in sourcecount:  # count of the stories by source
        sourcecount[source] += 1
    else:
        sourcecount[source] = 1

    sentlist = sentence_segmenter(story)
    
    nsent = 1
    for sent in sentlist:
        if sent[0] != '"':  # skip sentences beginning with quotes
            print thisdate, source, thisURL
            fout.write(thisdate + ' ' + source + '-' +
                       str(sourcecount[source]).zfill(4) + '-' + str(nsent) + ' ' + thisURL + '\n')
            lines = textwrap.wrap(sent, 80)
            for txt in lines:
                fout.write(txt + ' \n')
#				print txt
            fout.write('\n')
#			print '\n',
        nsent += 1
        if nsent > MAX_LEDE: break

def sentence_segmenter(paragr):
	""" Breaks the string 'paragraph' into a list of sentences based on the following rules
	1. Look for terminal [.,?,!] followed by a space and [A-Z]
	2. If ., check against abbreviation list ABBREV_LIST: Get the string between the . and the
	   previous blank, lower-case it, and see if it is in the list. Also check for single-
	   letter initials. If true, continue search for terminal punctuation
	3. Extend selection to balance (...) and "...". Reapply termination rules
	4. Add to sentlist if the length of the string is between MIN_SENTLENGTH and MAX_SENTLENGTH 
	5. Returns sentlist """

#	ka = 0
#	print '\nSentSeg-Mk1'
	sentlist = []
	searchstart = 0  # controls skipping over non-terminal conditions
	terloc = terpat.search(paragr)
	while terloc:
#		print 'Mk2-0:', paragr[:terloc.start()+2]
		isok = True
		if paragr[terloc.start()] == '.':
			if (paragr[terloc.start()-1].isupper() and 
				paragr[terloc.start()-2] == ' '):  isok = False   # single initials
			else:
				loc = paragr.rfind(' ',0,terloc.start()-1)   # check abbreviations
				if loc > 0:
#					print 'SentSeg-Mk1: checking',paragr[loc+1:terloc.start()+1]
					if paragr[loc+1:terloc.start()+1].lower() in ABBREV_LIST: 
#						print 'SentSeg-Mk2: found',paragr[loc+1:terloc.start()+1]
						isok = False
		if paragr[:terloc.start()].count('(') != paragr[:terloc.start()].count(')') :  
#			print 'SentSeg-Mk2: unbalanced ()'
			isok = False
		if paragr[:terloc.start()].count('"') % 2 != 0  :
#			print 'SentSeg-Mk2: unbalanced ""'
			isok = False
		if isok:
			if (len(paragr[:terloc.start()]) > MIN_SENTLENGTH and   
				len(paragr[:terloc.start()]) < MAX_SENTLENGTH) :
				sentlist.append(paragr[:terloc.start()+2])
#				print 'SentSeg-Mk3: added',paragr[:terloc.start()+2]
			paragr = paragr[terloc.end()-1:]
			searchstart = 0
		else: searchstart = terloc.start()+2 
 
#		print 'SentSeg-Mk4:',paragr[:64]
#		print '            ',paragr[searchstart:searchstart+64]
		terloc = terpat.search(paragr,searchstart)
#		ka += 1
#		if ka > 16: sys.exit()

	if (len(paragr) > MIN_SENTLENGTH and len(paragr) < MAX_SENTLENGTH) :  # add final sentence 
		sentlist.append(paragr) 
		
	return sentlist



def format_csmonit():
    global story, source, thisdate
#	global cssplit
#	m = cssplit.match(story)
#	print '==', m.group(0),  m.group(1), m.group(2)
#	story = cssplit.sub('\1. \2',story)
    source = 'CSM'
    tifld = thisURL.split('/')  # convert the date to YYMMDD
    ka = 1
    while not tifld[ka].startswith('20'):
        ka += 1
    tistr = tifld[ka] + ' ' + tifld[ka + 1][:2] + ' ' + tifld[ka + 1][2:]
#		print tistr
    strctim = time.strptime(tistr, '%Y %m %d')
    thisdate = time.strftime('%y%m%d', strctim)
#	print story


def format_plain(srcstr):
    global source, thisdate
    thisdate = get_plaindate()
    source = srcstr


def format_fmt(srcstr):
    global source, thisdate
    thisdate = get_fmtdate()
    source = srcstr


def format_google():
    global story, source, thisdate
    thisdate = get_fmtdate()
    if '(Reuters)' in story:
        story = story[story.find('(Reuters)') + 11:]
        source = 'REU'
    else:
        source = 'GOG'

def format_bbc():
    global source, thisdate
    if story.startswith('This page is best viewed in an'):
        source = ''
    else:
        thisdate = get_fmtdate()
        source = 'BBC'


def format_nyt():
    global source, thisdate, story
    thisdate = get_fmtdate()
    source = 'NYT'
# unhappy with the non-ASCII char


def format_reuters():
    global source, thisdate, story
    thisdate = get_fmtdate()
    source = 'REU'
    if '(Reuters)' in story:
        story = story[story.find('(Reuters)') + 13:]

def format_yahoo():
    global source, thisdate, story
    thisdate = get_fmtdate()
    source = 'YAH'
    if '(IANS)' in story:
        story = story[story.find('(IANS)') + 7:]
    elif '(ANI)' in story:
        story = story[story.find('(ANI)') + 6:]


def format_xinhua():
    global source, thisdate, story
    thisdate = get_plaindate()
    source = 'XIN'
    if '(Xinhua) -- ' in story:
        story = story[story.find('(Xinhua) -- ') + 13:]


def format_upi():
    global source, thisdate, story
    thisdate = get_fmtdate()
    source = 'UPI'
    if '(UPI) -- ' in story:
        story = story[story.find('(UPI) -- ') + 9:]


# ============ main program =============== #

def main(thisday):
    global field, story, source, thisdate, thisURL, sourcecount, fout

    scraperfilename = scraperstem + thisday + '.txt'
    print "Mongo: Scraper file name:", scraperfilename

    recordfilename = recordfilestem + thisday + '.txt'
    print "Mongo: Record file name:", recordfilename

    try:
        fin = open(scraperfilename, 'r')
    except IOError:
        print "\aError: Could not find the event file"
        sys.exit()

    fout = open(recordfilename, 'w')
    sourcecount = {}

    storyno = 1
    csno = 1
    line = fin.readline()
    while len(line) > 0:  # loop through the file
        field = line.split('\t')
#        print field
        thisURL = field[2][:-1]
#    	print thisURL
        story = fin.readline()  # *usually* the entire story will be in one line
        line = fin.readline()  # skip blank line
        while len(line) > 0 and '\thttp://' not in line:  #  but add additional lines to story: this is occurring in some cases
        	if len(line) > 2:
        		story += ' ' + line 
        	line = fin.readline()    # line will be a header

        if 'www.csmonitor.com' in thisURL:
            format_csmonit()
        elif 'www.bbc.co.uk' in thisURL:
            format_bbc()
        elif 'feeds.reuters.com' in thisURL:
            format_reuters()
        elif 'news.xinhuanet.com' in thisURL:
            format_xinhua()
        elif 'www.upi.com' in thisURL:
            format_upi()
        elif ('www.nytimes.com' in thisURL or
              'thelede.blogs.nytimes.com' in thisURL):
            format_nyt()
        elif ('news.google.com' in thisURL or
              'feedproxy.google.com' in thisURL):
            format_google()
        elif 'www.todayszaman.com' in thisURL:
            format_plain('TZA')
        elif 'hosted2.ap.org' in thisURL:
            format_plain('APP')
        elif 'www.theguardian.com' in thisURL:
            format_plain('GUA')
        elif 'www.todayszaman.com' in thisURL:
            format_plain('TZA')
        elif 'www.insightcrime.org' in thisURL:
            format_fmt('ISC')
        elif 'www.france24.com' in thisURL:
            format_fmt('FRA')
        elif 'in.news.yahoo.com' in thisURL:
            format_yahoo()
        elif 'allafrica.com' in thisURL:
            format_fmt('ALA')
        elif 'xxx-africa.com' in thisURL:
            format_fmt('XXA')
        elif 'xxx-africa.com' in thisURL:
            format_fmt('XXA')
        elif 'xxx-africa.com' in thisURL:
            format_fmt('XXA')
        else:
            source = ''  # general trigger for a skip
            		
        if (len(source) > 0) and (thisday == thisdate):
            write_record()

    #	storyno += 1
    #	if storyno > 8: sys.exit()

    fin.close()
    fout.close()
    print "Finished"

if __name__ == '__main__':
    if len(sys.argv) > 1:
        thisday = sys.argv[1]
    else:
        print 'Error: No date suffix in Mongo.formatter.py'
        sys.exit()

    main(thisday)
