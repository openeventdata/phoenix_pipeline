.. Phoenix Pipeline documentation master file, created by
   sphinx-quickstart on Thu Apr 17 17:07:15 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Introduction
============
Turning news into events since 2014.

Welcome to the documentation for the Phoenix (PHOX) Pipeline! The PHOX pipeline
is a system that links a series of Python programs to convert files from a
whitelist of RSS feeds into event data and uploads the data into a designated
server. This reference provides a description of how the pipeline works and
what it does. It is also written as a programming reference including necessary
details on packages, modules, and classes needed to contribute to the codebase.


How it Works
============
The PHOX pipeline links a series of Python programs to convert files scrapped
from a whitelist of RSS feeds to machine-coded event data using the `PETRARCH
<http://petrarch.readthedocs.org/en/latest/>`_ event data coding software. The
generated event data is then uploaded to a server designated in a config file.
The system is designed to process a single days worth of information that can
be included in multiple text files. Below is a flowchart of the pipeline:

.. image:: phox_pipeline_flow.jpeg


Source code can be found at: https://github.com/openeventdata/phoenix_pipeline

This software is MIT Licensed (MIT)
Copyright (c) 2014 Open Event Data Alliance



Contents:

.. toctree::
   :maxdepth: 2

   details
   contribute
   pipeline

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
