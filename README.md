phoenix_pipeline
================

[![Build Status](https://travis-ci.org/openeventdata/phoenix_pipeline.svg?branch=master)](https://travis-ci.org/openeventdata/phoenix_pipeline)
[![Join the chat at https://gitter.im/openeventdata/phoenix_pipeline](https://badges.gitter.im/openeventdata/phoenix_pipeline.svg)](https://gitter.im/openeventdata/phoenix_pipeline?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Turning news into events since 2014.

This system links a series of Python programs to convert the files which have been 
downloaded by a [web scraper](https://github.com/openeventdata/scraper) to coded event data which is uploaded to a web site
designated in the config file. The system processes a single day of information, but this 
can be derived from multiple text files. The pipeline also implements a filter for
source URLs as defined by the keys in the `source_keys.txt` file. These keys
correspond to the `source` field in the MongoDB instance.

For more information please visit the [documentation](http://phoenix-pipeline.readthedocs.org/en/latest/).

##Running

To run the program:

    python pipeline.py
