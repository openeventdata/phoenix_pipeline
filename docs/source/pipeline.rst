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
        
:mod:`oneaday_formatter` Module
-------------------------------
Deduplication and other useful formatting functions for the final output. Reads
in a single day of coded event data, selects first record of souce-target-event
combination and records references for any additional events of same
source-target-event combination.
        
.. automodule:: oneaday_formatter
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
