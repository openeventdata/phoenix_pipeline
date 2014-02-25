# phox_uploader.py
##
# This uses ftp to update and upload the new daily events to a web site that has daily, 
# monthly and annual files. The destination site is reached via the path serverdir and 
# is assumed to have subdirectories labelled "Daily", "Daily/Duplicates","Monthly" and 
# "Annual." The daily and duplicate files are simply zipped and uploaded. The daily data 
# are appended to existing monthly and yearly files without the DOC DOC 999 records 
# except on the first day of the month or  year, when the event file becomes the new file 
# (with the documentation). The first names are determined by names in the config.ini file 
# and the date string.
#
# This was developed to run as part of the phoenix_pipeline system.
#
##
# TO RUN PROGRAM: python phox_uploader.py YYMMDD
#
# where YYMMDD is the year, month and day that will be used in the file names
##
# INPUT FILES: 
#	eventfile_stem+YYMMDD+.txt  (e.g. Phoenix.events.140215.txt)
#	dupfile_stem+YYMMDD+.txt  (e.g. Phoenix.dupindex.140215.txt)
##
# OUTPUT FILES: none, but the following files are modified or created on the server
#   outputfile_stem+YY+'-'+MM+'.txt.zip'
#   outputfile_stem+YY+'.txt.zip'
##
# PROGRAMMING NOTES:
# 1. This leaves the various zipped files on the server as well as transferring them
#    to the web site; this may or may not be a good idea, and they probably should be
#    transferred to some subdirectory.
##
# SYSTEM REQUIREMENTS
# This program has been successfully run under Mac OS 10.6 and Ubuntu 12.2 (Linode).
##
#	PROVENANCE:
#	Programmer: Philip A. Schrodt
#				Parus Analytical Systems
#				schrodt735@gmail.com
#				http://eventdata.parsuanalytics.com
#
# Copyright (c) 2014	Philip A. Schrodt.	All rights reserved.
##
# This project was funded in part by National Science Foundation grant SES-1004414
##
# Redistribution and use in source and binary forms, with or without modification,
# are permitted under the terms of the MIT License: http://opensource.org/licenses/MIT
##
# Report bugs to: schrodt735@gmail.com
# Source Code Location: https://github.com/openeventdata/phoenix_pipeline
##
# REVISION HISTORY:
# 18-Feb-14:	Initial version
##
# ------------------------------------------------------------------------

import sys
import subprocess
import phox_utilities
from ftplib import FTP

def store_zipped_file(filename, dirname):
	""" 
	Zips and uploads the file filename into the subdirectory dirname, then cd back out 
	to parent directory
	Exits on error and raises RuntimeError 
	"""
	global ftp 
	filezip = filename + '.zip'
	try:
		subprocess.call("zip " + filezip + ' ' + filename, shell=True)
		ftp.cwd(dirname)               # change into subdirectory
		ftp.storbinary("STOR " + filezip, open(filezip))
		ftp.cwd('..')               # back out
	except:   
		phox_utilities.do_RuntimeError('Store of',  filename, '.zip unsuccessful')

def get_zipped_file(filename, dirname):
	""" 
	Downloads the file filename+zip from the subdirectory dirname, reads into 
	tempfile.zip, cds back out to parent directory and unzips
	Exits on error and raises RuntimeError 
	"""
	global ftp 
	fbin = open('tempfile.zip','wb')
	try:
		ftp.cwd(dirname)               # change into subdirectory
		ftp.retrbinary("RETR " + filename + '.zip', fbin.write)
		ftp.cwd('..')               # back 
		phox_utilities.logger.info('Successfully retrieved ' + filename + '.zip\n')
	except:   
		phox_utilities.do_RuntimeError('Retrieval of',  filename, '.zip unsuccessful')
		return
	
	fbin.close()
	try:
		subprocess.call("unzip -o tempfile.zip", shell=True)  # -o: overwrite without prompting
		subprocess.call("rm tempfile.zip", shell=True)  # clean up
	except:   
		phox_utilities.do_RuntimeError('Downloaded file',  filename, 'could not be decompressed')

def main(datestr):
	""" 
	When something goes amiss, various routines will and pass through a 
	RuntimeError(explanation) rather than trying to recover, since this probably means
	something is either wrong with the ftp connection or the file structure got 
	corrupted. This error is logged but needs to be caught in the calling program.
	"""
	global ftp	
	
	# log into the server
	try:
		ftp = FTP(phox_utilities.Server_List[0])     # connect to host, default port
		ftp.login(phox_utilities.Server_List[1], phox_utilities.Server_List[2])
		ftp.cwd(phox_utilities.Server_List[3])               # change into PHOX directory
		print 'Logged into:', phox_utilities.Server_List[0], '/', phox_utilities.Server_List[1]
	except:
		phox_utilities.do_RuntimeError('Login to', phox_utilities.Server_List[0], 'unsuccessful')

	# upload the daily event and duplicate index files
	try:
		eventfilename = phox_utilities.Eventfile_Stem + datestr + '.txt'
		store_zipped_file(eventfilename,'Daily')
	except:
		phox_utilities.do_RuntimeError('Transfer of', eventfilename,'unsuccessful')

	try:
		dupfilename = phox_utilities.Dupfile_Stem + datestr + '.txt'
		store_zipped_file(dupfilename,'Daily/Duplicates')
		ftp.cwd('..')               # back out one more level
	except:
		phox_utilities.do_RuntimeError('Transfer of', dupfilename,'unsuccessful')


	# update the monthly and yearly files
	monthfilename = phox_utilities.Outputfile_Stem + datestr[:2] + '-' + datestr[2:4] + '.txt'
	yearfilename = phox_utilities.Outputfile_Stem + datestr[:2] + '.txt'

	curyear = True
	if datestr[2:] == '0101': # initialize a new year
		subprocess.call("cp " + eventfilename + ' ' + yearfilename, shell=True)
		curyear = False  
	else:
		get_zipped_file(yearfilename, 'Annual')
		try:
			fyr = open(yearfilename,'a')  # this actually becomes a simple write rather than append when a new month  
		except:   
			phox_utilities.do_RuntimeError('Could not open yearly file', yearfilename)


	curmonth = True
	if datestr[4:] == '01': # just make a copy of the existing file with the DOC lines
		subprocess.call("cp " + eventfilename + ' ' + monthfilename, shell=True)  
		curmonth = False	
	else: # download existing files and append to it
		get_zipped_file(monthfilename, 'Monthly')
		try:
			fmon = open(monthfilename,'a')  # this actually becomes a simple write rather than append when a new month  
		except:   
			phox_utilities.do_RuntimeError('Could not open monthly file', monthfilename)

	if curyear or curmonth:
		try:
			fin = open(eventfilename,'r')   
		except:   
			phox_utilities.do_RuntimeError('Could not open the daily event file', eventfilename)

		line = fin.readline()
		while len(line) > 0:  # loop through the file
			if 'DOC\tDOC\t999' not in line:   # copy the new lines, skipping the documentation lines
				if curmonth: fmon.write(line)
				if curyear:  fyr.write(line)  
			line = fin.readline()

		fin.close()
		if curmonth: fmon.close()
		if curyear: fyr.close()

	store_zipped_file(monthfilename, 'Monthly')
	store_zipped_file(yearfilename, 'Annual')

	ftp.quit()
	print "Finished"

if __name__ == '__main__':
	if len(sys.argv) > 1:
		datestr = sys.argv[1]
	else:
		print 'Error: No date provided to PHOX_uploader.py'
		sys.exit()

	try: main(datestr)
	except RuntimeError as why:
		print 'phox_uploader.main() raised the RuntimeError:',why
