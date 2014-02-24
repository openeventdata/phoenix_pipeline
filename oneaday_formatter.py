# oneaday_formatter.py
##
# One-A-Day filter with count of duplicates and list of their IDs
##
# TO RUN PROGRAM AS STANDALONE:
##
# python oneaday_formatter.py <date_string> -s
#      where <date_string> is of the form YYMMDD. 
#      Second command line option triggers standalong run: see notes
##
# INPUT FILES: phox_utilities.Fullfile_Stem + date_string + '.txt'
#      Unfiltered events in TABARI format
##
# OUTPUT FILES:
# 	Events: phox_utilities.Eventfile_Stem + datestr + '.txt'
#      	Filtered events in TABARI format
#  
#   Duplicates: phox_utilities.Dupfile_Stem + datestr + '.txt'.index.<day>.txt"
#		Index for duplicates for each day found in the file: see Phoenix documentation
##
# PROGRAMMING NOTES:
# 1. This program eventually goes into the Phoenix pipeline and is called using 
#    oneaday_formatter.main(date_string). In standalone mode for development, use a 
#    second command line option (anything): this will set up the logger and phox_utilities
##
# SYSTEM REQUIREMENTS
# This program has been successfully run under Mac OS 10.6; it is standard Python 2.5
# so it should also run in Unix or Windows.
##
# PROVENANCE:
#	Programmer: Philip A. Schrodt
#				Parus Analytical Systems
#				schrodt735@gmail.com
#				http://eventdata.parsuanalytics.edu
#
# Copyright (c) 2014	Philip A. Schrodt.	All rights reserved.
##
# This project was funded in part by National Science Foundation grant SES-1004414
##
# Redistribution and use in source and binary forms, with or without modification,
# are permitted under the terms of the MIT License: http://opensource.org/licenses/MIT
##
# Report bugs to: schrodt735@gmail.com
##
# REVISION HISTORY:
# 01-Feb-14:	Initial version
# 23-Feb-14:	Incorporates phox_utilities to handle file names (pas)
##
# ------------------------------------------------------------------------

import sys
import phox_utilities

# ======== global initializations ========= #

evtdict = {}  # initialize the dictionaries
evtdup = {}

DUPCOUNT = 7  # field index for the duplicate count
URLLOC = 6	# field index for the URL

def writeevents():
	global fout, evtdict, DUPCOUNT
	for locevt, loclist in evtdict.iteritems():
		fout.write(evtdict[locevt][0] + '\t')

		if len(evtdict[locevt][1]) == 3:
			fout.write(evtdict[locevt][1] + '\t\t\t')  # source + agents[s]
		elif len(evtdict[locevt][1]) == 6:
			fout.write(evtdict[locevt][1][:3] + '\t' +
					   evtdict[locevt][1][3:] + '\t\t')
		else:
			fout.write(evtdict[locevt][1][:3] + '\t' + evtdict[locevt]
					   [1][3:6] + '\t' + evtdict[locevt][1][6:] + '\t')

		if len(evtdict[locevt][2]) == 3:
			fout.write(evtdict[locevt][2] + '\t\t\t')  # target + agents[s]
		elif len(evtdict[locevt][2]) == 6:
			fout.write(evtdict[locevt][2][:3] + '\t' +
					   evtdict[locevt][2][3:] + '\t\t')
		else:
			fout.write(evtdict[locevt][2][:3] + '\t' + evtdict[locevt]
					   [2][3:6] + '\t' + evtdict[locevt][2][6:] + '\t')

		fstr = '\t'.join(evtdict[locevt][3:DUPCOUNT])
		# add the duplicate count
		fout.write(fstr + '\t' + str(evtdict[locevt][DUPCOUNT]))
		if evtdict[locevt][DUPCOUNT] > 1:
			fout.write('\t')   # write duplicate sources and their frequencies
			if locevt in evtdup:
				for loclist in evtdup[locevt]:
					fout.write(' ' + loclist[0] + ' ' + str(loclist[1]))
				fout.write('\n')
			else:
				fout.write('\tMissing references\n')  # should not hit this
		else:
			fout.write('\t\n')	# no duplicates, so blank field


def writedups(datestr):
	global evtdict, evtdup, curday
	fdup = open(phox_utilities.Dupfile_Stem + datestr + '.txt', 'w')
	for locevt, loclist in evtdup.iteritems():
		if len(loclist) > 0:
			fstr = '\t'.join(evtdict[locevt][:DUPCOUNT])
			fdup.write(fstr + "\n")
			for ka in range(len(loclist)):
#				print '++',ka, loclist
				fdup.write("\t" + loclist[ka][0] + " " + str(loclist[ka][1]))
				for kb in range(len(loclist[ka][2:])):
#					print '--',kb, loclist[ka][kb+2]
					fdup.write(" " + loclist[ka][kb + 2])
				fdup.write("\n")
			fdup.write("\n")
	fdup.close()


def main(datestr):
	global fout, evtdict, DUPCOUNT, evtdup, curday
	try:
		fin = open(phox_utilities.Fullfile_Stem + datestr + '.txt', 'r')
	except IOError:
		phox_utilities.do_RuntimeError('Could not find the full event file for', datestr)

	eventfilename = phox_utilities.Eventfile_Stem + datestr + '.txt'
	fout = open(eventfilename, 'w')
	print 'Writing', eventfilename

	curday = '000000'
	dayno = 1
	line = fin.readline()
	while len(line) > 0:  # loop through the file
		field = line[:-1].split('\t')
#		print '--',field
		if field[0] != curday:
			writeevents()
			if curday != '000000':
				writedups(curday)
			curday = field[0]
			evtdict = {}
			evtdup = {}

		# string to check against for duplicates
		evt = field[1] + field[2] + field[3]
		src = field[5][0:3]
		field[6] = field[6][:16]  # debug -- readability
		field.append(1)
#		print evt
		if evt in evtdict:	# duplicate
#			print '++',field
			evtdict[evt][DUPCOUNT] += 1
	#		print evt, evtdict[evt][5], evtdict[evt][6]
			gotsrc = False
			for ka in range(len(evtdup[evt])):
				if evtdup[evt][ka][0] == src:
					evtdup[evt][ka][1] += 1
					evtdup[evt][ka].append(field[5])
					evtdup[evt][ka].append(field[6])
					gotsrc = True
					break
			if not gotsrc:	# new source
				evtdup[evt].append([src, 1, field[5], field[6]])

		else:
			evtdict[evt] = field
			evtdup[evt] = []

		dayno += 1
	#	if dayno > 128: sys.exit()   # debug

		line = fin.readline()

	fin.close()

	writeevents()  # write final day (which is only day in the pipeline call)
	fout.close()

	writedups(datestr)

	print "Finished"

if __name__ == '__main__':
	if len(sys.argv) > 2:   # initializations for stand-alone tests
		phox_utilities.init_logger('test_pipeline.log')
		logger = phox_utilities.logger  # get a local copy for the pipeline
		phox_utilities.parse_config('test_config.ini') # initialize the various phox_utilities globals

	if len(sys.argv) > 1:
		datestr = sys.argv[1]
	else:
		print 'Date string is required for oneaday_formatter.py'
		sys.exit()

	main(datestr)
