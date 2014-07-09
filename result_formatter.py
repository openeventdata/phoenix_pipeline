from __future__ import print_function
from __future__ import unicode_literals
import logging
from collections import Counter


def filter_events(results):
    """
    Filters out duplicate events, leaving only one unique
    (DATE, SOURCE, TARGET, EVENT) tuple per day.


    Parameters
    ----------

    results: Dictionary.
                PETRARCH-formatted results in the
                {StoryID: [(record), (record)]} format.


    Returns
    -------

    formatted_dict: Dictionary.
                    Contains filtered events. Keys are
                    (DATE, SOURCE, TARGET, EVENT, COUNTER) tuples,
                    values are lists of IDs, sources, and issues. The
                    ``COUNTER`` in the tuple is a hackish workaround since each
                    key has to be unique in the dictionary and the goal is to
                    have every coded event appear event if it's a duplicate.
                    Other code will just ignore this counter.
    """
    formatted = {}
    for story in results:
        for event in results[story]:
            date = event[0]
            src = event[1]
            target = event[2]
            code = event[3]
            if len(event) == 7:
                ids = event[4].split(';')
                url = event[5]
                source = event[6]
                issues = ''
            else:
                issues = event[4]
                issues = issues.split(';')
                ids = event[5].split(';')
                url = event[6]
                source = event[7]

            event_tuple = (date, src, target, code, 0)

            counter = 0
            while True:
                if event_tuple in formatted:
                    counter += 1
                    event_tuple = (date, src, target, code, counter)
                else:
                    break

            formatted[event_tuple] = {'issues': Counter(), 'ids': ids,
                                      'sources': [source], 'urls': [url]}
            if issues:
                issue_splits = [(iss, c) for iss, c in
                                [issue_str.split(',') for issue_str in
                                    issues]]
                for issue, count in issue_splits:
                    formatted[event_tuple]['issues'][issue] += int(count)

    return formatted


def main(results):
    """
    Pulls in the coded results from PETRARCH dictionary in the
    {StoryID: [(record), (record)]} format and converts it into
    (DATE, SOURCE, TARGET, EVENT, COUNTER) tuple format. The ``COUNTER`` in the
    tuple is a hackish workaround since each key has to be unique in the
    dictionary and the goal is to have every coded event appear event if it's a
    duplicate.  Other code will just ignore this counter. Returns this new,
    filtered event data.

    Parameters
    ----------

    results: Dictionary.
                PETRARCH-formatted results in the
                {StoryID: [(record), (record)]} format.


    Returns
    -------

    formatted_dict: Dictionary.
                    Contains filtered events. Keys are
                    (DATE, SOURCE, TARGET, EVENT, COUNTER) tuples,
                    values are lists of IDs, sources, and issues. The
                    ``COUNTER`` in the tuple is a hackish workaround since each
                    key has to be unique in the dictionary and the goal is to
                    have every coded event appear event if it's a duplicate.
                    Other code will just ignore this counter.
    """
    logger = logging.getLogger('pipeline_log')

    logger.info('Formatting PETRARCH results.')
    formatted = filter_events(results)

    return formatted
