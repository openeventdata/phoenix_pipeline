phoenix_pipeline
================

[![Build Status](https://travis-ci.org/openeventdata/phoenix_pipeline.svg?branch=master)](https://travis-ci.org/openeventdata/phoenix_pipeline)
[![Join the chat at https://gitter.im/openeventdata/phoenix_pipeline](https://badges.gitter.im/openeventdata/phoenix_pipeline.svg)](https://gitter.im/openeventdata/phoenix_pipeline?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Turning news into events since 2014.


This system links a series of Python programs to convert the files which have been 
downloaded by a [web scraper](https://github.com/openeventdata/scraper) to
coded event data which is uploaded to a web site designated in the config file.
The system processes a single day of information, but this can be derived from
multiple text files. The pipeline also implements a filter for source URLs as
defined by the keys in the `source_keys.txt` file. These keys correspond to the
`source` field in the MongoDB instance.

For more information please visit the [documentation](http://phoenix-pipeline.readthedocs.org/en/latest/).


## Requirements

The pipeline requires either
[Petrarch](https://github.com/openeventdata/petrarch) or
[Petrarch2](https://github.com/openeventdata/petrarch2) to be installed. Both
are Python programs and can be installed from Github using pip.

The pipeline assumes that stories are stored in a MongoDB in a particular
format. This format is the one used by the OEDA news RSS scraper. See [the
code](https://github.com/openeventdata/scraper/blob/master/mongo_connection.py)
for details on it structures stories in the Mongo. Using this pipeline with
differently formatted databases will require changing field names throughout
the code. The pipeline also requires that stories have been parsed with
Stanford CoreNLP. See the [simple and
stable](https://github.com/openeventdata/stanford_pipeline) way to do this, or
the [experimental distributed](https://github.com/oudalab/biryani) approach.

The pipeline requires one of two geocoding systems to be running: CLIFF-CLAVIN
or Mordecai. For CLIFF, see a VM version
[here](https://github.com/ahalterman/CLIFF-up) or a Docker container version
[here](https://github.com/openeventdata/cliff_container). For Mordecai, see the
setup instructions [here](https://github.com/openeventdata/mordecai). The
version of the pipeline deployed in production currently uses CLIFF/CLAVIN, but
future development will focus on improvements to Mordecai.

## Configuration

The pipeline has two configuration files. `PHOX_config.ini` specifies which
geolocation system to use, how to name the files produced by the pipeline, and
how to upload the files to a remote server if desired.

`petr_config.ini` is the configuration file for Petrarch2 itself, including the
location of dictionaries, new actor extraction options, and the one-a-day filter. For
more details see the main [Petrarch2 repo](https://github.com/openeventdata/petrarch2/).

## Running

To run the program:

```
python pipeline.py
```


