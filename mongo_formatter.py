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
# 22-Feb-14:    Revision by MYI
##
# ------------------------------------------------------------------------

import sys
import time
import textwrap
import re
from dateutil import parser

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

sources = {'csmonitor.com':'CSM', 'bbc.co.uk':'BBC', 'reuters.com':'REU', 'xinhuanet.com':'XIN', 
  'www.upi.com':'UPI', 'nytimes.com':'NYT', 'todayszaman.com':'TZA', 'hosted2.ap.org':'APP', 
  'theguardian.com':'GUA', 'todayszaman.com':'TZA', 'insightcrime.org':'ISC', 'france24.com':'FRA', 
  'yahoo.com':'YAH', 'allafrica.com':'ALA', 'voanews.com':'VOA', 'aljazeera.com':'AJZ', 'AlAkhbarEnglish':'AKB', 
  'usatoday.com':'USA', 'latimes.com':'LAT', 'foxnews.com':'FOX','IRINnews.org':'IRI', 'rfi.fr':'RFI', 'cnn.com':'CNN',  
  'abcnews':'ABC', 'wsj.com':'WSJ', 'nydailynews.com':'NYD','washingtonpost.com':'WAS', 'chicagotribune.com': 'CHT'}

scraperstem = 'scraper_results_20'
recordfilestem = 'eventrecords.'
newsourcestem = 'newsources.'

# ======== functions ========= #


def get_date(field):
    if 'csmonitor.com' in field[-1]:
        csmdate = field[-1].split('/20')[1].split('/')
        return csmdate[0]+csmdate[1]
    elif 'latimes.com' in field[-1]:
        latdate = field[-1].split('-20')[-1].split(',')
        return latdate[0]
    elif 'foxnews.com' in field[-1]:
        foxdate = field[-1].split('http://')[-1].split('/')[2:5]
        return ''.join(foxdate)[2:]
    elif 'rfi.fr' in field[-1]:
        rfidate = field[-1].split('/')[-1].split('-')[0]
        return rfidate[2:]
    elif 'cnn.com' in field[-1]:
        if 'interactive/' in field[-1]:
            date_obj = parser.parse(field[1])
            return str(date_obj)[2:4] + str(date_obj)[5:7] + str(date_obj)[8:10]
        else:
            cnndate = field[-1].split('/20')[-1].split('/')
            return cnndate[0]+ cnndate[1] + cnndate[2]
    elif field[1]:
        date_obj = parser.parse(field[1])
        return str(date_obj)[2:4] + str(date_obj)[5:7] + str(date_obj)[8:10]
    else:
        return '000000'


def write_record(source, sourcecount, thisdate, thisURL, story, fout):
    if source in sourcecount:  # count of the stories by source
        sourcecount[source] += 1
    else:
        sourcecount[source] = 1

    sentlist = sentence_segmenter(story)
    
    nsent = 1
    for sent in sentlist:
        if sent[0] != '"':  # skip sentences beginning with quotes
            print thisdate, source, thisURL
            print >> fout, thisdate + ' ' + source + '-' + str(sourcecount[source]).zfill(4) + '-' + str(nsent) + ' ' + thisURL #+ '\n'

            lines = textwrap.wrap(sent, 80)
            for txt in lines:
                print >> fout, txt
            
            print >> fout, '\n'

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



def get_source(field):
  source_url = field[-1].strip()
  source = source_url.split('http://')[-1]
  
  try:
      key = [i for i, s in enumerate(sources.keys()) if s in source]
      if key:
          return sources[sources.keys()[key[0]]]
      else:
          return '999'
  except AttributeError:
      return '999'

def get_story(story):
  if '(Reuters)' in story:
    return story[story.find('(Reuters)') + 12:]
  elif '(IANS)' in story:
    return story[story.find('(IANS)') + 7:]
  elif '(ANI)' in story:
    return story[story.find('(ANI)') + 7:]
  elif '(Xinhua) -- ' in story:
    return story[story.find('(Xinhua) -- ') + 12:]
  elif '(UPI) -- ' in story:
    return story[story.find('(UPI) -- ') + 9:]
  else:
    return story


# ============ main program =============== #

def main(thisday):
    scraperfilename = scraperstem + thisday + '.txt'
    print "Mongo: Scraper file name:", scraperfilename

    recordfilename = recordfilestem + thisday + '.txt'
    print "Mongo: Record file name:", recordfilename

    newsourcefile = newsourcestem + thisday + '.txt'
    print "Mongo: New Sources file name:", newsourcefile

    try:
        fin = open(scraperfilename, 'r')
    except IOError:
        print "\aError: Could not find the event file"
        sys.exit()

    finlist = fin.readlines()
    fout = open(recordfilename, 'w')
    newout = open(newsourcefile, 'w')
    sourcecount = {}

    storyno = 1
    csno = 1

    for line in range(0, len(finlist)):
      if 'http' in finlist[line]:
        field = finlist[line].split('\t')
        thisURL = field[2][:-1]
            
        thisstory = get_story(finlist[line+1])
        thisdate = get_date(field)
        thissource = get_source(field)  

        if thissource == '999':
          print >> newout, thisURL    #Adds sources not included in sources dictionary to 'newsource_results_20..' file output
        
        write_record(thissource, sourcecount, thisdate, thisURL, thisstory, fout)

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