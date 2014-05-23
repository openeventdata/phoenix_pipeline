phoenix_pipeline
================

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
