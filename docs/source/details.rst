Pipeline Details
================


Configuration File
------------------

PHOX_config.ini configures the initial settings for PHOX pipeline and should be included in the working directory.

::

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

Example of PHOX_config.ini
    
::

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

TABARI.0.8.4b2
------------------------

TABARI (Text Analysis By Augmented Replacement Instructions) is an event coding program used to machine code even data from formatted source texts in the pipeline. It is dictionary-based and relies on sparse parsing and pattern recognition to identify 'who-did-what-to-whom' relations. 

For details see: http://eventdata.parusanalytics.com/software.dir/tabari.html
