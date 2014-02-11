# OneADay.reformatter.py
##
# One-A-Day filter with count of duplicates and list of their IDs
##
# TO RUN PROGRAM:
##
# python OneADay.reformatter.py
##
# INPUT FILES: hardcoded to PHOX.filelist.txt
##
# OUTPUT FILES: "duplicates.txt", list of the duplicates with file names
##
# PROGRAMMING NOTES: None
##
# SYSTEM REQUIREMENTS
# This program has been successfully run under Mac OS 10.6; it is standard Python 2.5
# so it should also run in Unix or Windows.
##
# PROVENANCE:
# Programmer: Philip A. Schrodt
# Dept of Political Science
# Pennsylvania State University
# 227 Pond Laboratory
# University Park, PA, 16802 U.S.A.
# http://eventdata.psu.edu
##
# Copyright (c) 2012	Philip A. Schrodt.	All rights reserved.
##
# This project was funded in part by National Science Foundation grant
# SES-1004414
##
# Redistribution and use in source and binary forms, with or without modification,
# are permitted under the terms of the GNU General Public License:
# http://www.opensource.org/licenses/gpl-license.html
##
# Report bugs to: schrodt@psu.edu
##
# REVISION HISTORY:
# 01-Feb-14:	Initial version
##
# ------------------------------------------------------------------------

import sys
import time
import textwrap

# ======== global initializations ========= #


MAXLEDE = 4   # number of sentences to output
MIN_SENTENCE_LENGTH = 100   # minimum length of a sentence

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

    start = 0
    nsent = 1
    while (nsent < 5) and '. ' in story[start:]:
        end = story.find('. ', start)
        if (end - start > MIN_SENTENCE_LENGTH) and ('"' not in story[0:2]):
            print thisdate, source, thisURL
            fout.write(thisdate + ' ' + source + '-' +
                       str(sourcecount[source]).zfill(4) + '-' + str(nsent) + ' ' + thisURL + '\n')
            lines = textwrap.wrap(story[start:end + 2], 80)
            for txt in lines:
                fout.write(txt + ' \n')
#				print txt
            fout.write('\n')
#			print '\n',
        nsent += 1
        start = end + 2


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


def format_google():
    global story, source, thisdate
    thisdate = get_fmtdate()
    if '(Reuters)' in story:
        story = story[story.find('(Reuters)') + 10:]
        source = 'REU'
    else:
        source = ''


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
        source = ''


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
        thisURL = field[2][:-1]
    #	print thisURL
        story = fin.readline()
        line = fin.readline()  # skip blank line

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
        elif 'news.google.com' in thisURL:
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
            format_fmt('YAH')
        elif 'allafrica.com' in thisURL:
            format_fmt('ALA')
        elif 'allafrica.com' in thisURL:
            format_fmt('ALA')
        elif 'allafrica.com' in thisURL:
            format_fmt('ALA')
        elif 'allafrica.com' in thisURL:
            format_fmt('ALA')
        else:
            source = ''  # general trigger for a skip

        if (len(source) > 0) and (thisday == thisdate):
            write_record()

    #	storyno += 1
    #	if storyno > 8: sys.exit()

        line = fin.readline()

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
