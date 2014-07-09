Phoenix Pipeline Package
========================

:mod:`scraper_connection` Module
--------------------------------
Downloads scraped stories from Mongo DB.
                
.. automodule:: scraper_connection
    :members:
    :undoc-members:
    :show-inheritance:

:mod:`formatter` Module
-----------------------
Parses scraped stories from a Mongo DB into PETRARCH-formatted source text input.

.. automodule:: formatter
    :members:
    :undoc-members:
    :show-inheritance:
        
:mod:`oneaday_filter` Module
-------------------------------
Deduplication for the final output. Reads in a single day of coded event data,
selects first record of souce-target-event combination and records references
for any additional events of same source-target-event combination.
        
.. automodule:: oneaday_filter
    :members:
    :undoc-members:
    :show-inheritance:

:mod:`result_formatter` Module
-------------------------------
Puts the PETRARCH-generated event data into a format consistent with other
parts of the pipeline so that the events can be further processed by the
``postprocess`` module.
        
.. automodule:: result_formatter
    :members:
    :undoc-members:
    :show-inheritance:

:mod:`postprocess` Module
-------------------------------
Performs final formatting of the event data and writes events out to a text
file.
        
.. automodule:: postprocess
    :members:
    :undoc-members:
    :show-inheritance:

:mod:`geolocation` Module
-------------------------------
Geolocates the coded event data.
        
.. automodule:: geolocation
    :members:
    :undoc-members:
    :show-inheritance:

:mod:`uploader` Module
----------------------
Uploads PETRARCH coded event data and duplicate record references to designated server in config file.
                
.. automodule:: uploader 
    :members:
    :undoc-members:
    :show-inheritance:

:mod:`utilities` Module
-----------------------
Miscellaneous functions to do things like establish database connections, parse
config files, and intialize logging.
                
.. automodule:: utilities
    :members:
    :undoc-members:
    :show-inheritance:
