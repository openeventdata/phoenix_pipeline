phoenix_pipeline
================

[Note 24 Feb 2014: Still incomplete, but less so than it was at first... <pas>]

Turning news into events since 2014.

This system links a series of Python programs to convert the files which have been 
downloaded by scraper_connection.py to coded event data which is uploaded to a web site
designated in the config file. The system processes a single day of information, but this 
can be derived from multiple text files.

####PHOX_config.ini
This file should be in the working directory

```
[Server]
server_name = <server name for http: site>
username =  <user name for ftp login to server_name>
password =  <user password for ftp login to server_name>
server_dir = <path to directory on the server where subdirectories are located>

[Pipeline]
scraper_stem =  <stem for scrapped output>
recordfile_stem =  <stem for output of monger_formatter.py>
fullfile_stem =  <stem for output of TABARI.0.8.4b1>
eventfile_stem =  <stem for event output of oneaday_formatter.py>
dupfile_stem =  <stem for duplicate file output of oneaday_formatter.py>
outputfile_stem =  <stem for files uploaded by phox_uploader.py>
```

#####Example of PHOX_config.ini

```
[Server]
server_name = openeventdata.org
username = myusername
password = myweakpassword12345
server_dir = public_html/datasets/phoenix/

[Pipeline]
scraper_stem = scraper_results_20
recordfile_stem = eventrecords.
fullfile_stem = events.full.
eventfile_stem = Phoenix.events.
dupfile_stem = Phoenix.dupindex.
outputfile_stem = Phoenix.events.20
```

In the examples below, 'datestr' refers to a 6-digit YYMMDD date

####scraper_connection.py

Downloads formatted stories from a Mongo DB

```
Input: none
Output: files with the name scraper_stem + datestr + '.txt'
```


####monger_formatter.py

Downloads formatted stories from a Mongo DB

```
Command-line: 
Input: output files from scraper_connection.py
Output: TABARI-formatted records in a file with the name recordfile_stem + datestr + '.txt'
```


####TABARI.0.8.4b1
TABARI is a fully automated event data coder: details can be found at 

```
Command-line: 
Input, static: These are dictionary and configuration files used by the program
	CAMEO.091003.master.verbs
	nouns_adj_null.110124.txt
	Phoenix.140127.agents.txt
	Phoenix.Countries.140130.actors.txt
	Phoenix.Internatnl.140130.actors.txt
	Phoenix.MNSA.140131.actors.txt
	pipeline.options
Input, dynamic:
	TABARI-formatted records from monger_formatter.py
	PHOX.pipeline.project [this is updated with each run of the program]

Output: Coded events in a file with the name eventfile_stem + datestr + '.txt'
```


####oneaday_formatter.py

Deduplication on the basis of unique date-source-target-event tuples

```
Command-line: 
Input: TABARI event records
Output: Deduplicated events in a file with the name dupfile_stem + datestr + '.txt'
```


####phox_uploader.py

This uploads the file to a web site that has daily, monthly and annual files: see details 
on the assumed site structure in the program header. 

```
Command-line: date string
Input, static: default_config.ini [configuration file with information on the destination
   for the uploads
Input, dynamic: output files from  oneaday_formatter.py  
Output: log file 
```


####phox_pipeline.py

This is the glue program that put all of the above together.

```
Command-line: 
Input, static: default_config.ini 
Output: log file 
```


Source Code Location: https://github.com/openeventdata/phoenix_pipeline

Last update : 24 February 2014

Copyright (c) 2014 Open Event Data Alliance 
 
The MIT License (MIT)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.


