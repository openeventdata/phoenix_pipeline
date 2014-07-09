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

    filter_dict: Dictionary.
                    Contains filtered events. Keys are
                    (DATE, SOURCE, TARGET, EVENT) tuples, values are lists of
                    IDs, sources, and issues.
    """
    filter_dict = {}
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

            event_tuple = (date, src, target, code)

            if event_tuple not in filter_dict:
                filter_dict[event_tuple] = {'issues': Counter(), 'ids': ids,
                                            'sources': [source], 'urls': [url]}
                if issues:
                    issue_splits = [(iss, c) for iss, c in [issue_str.split(',')
                                                            for issue_str in
                                                            issues]]
                    for issue, count in issue_splits:
                        filter_dict[event_tuple]['issues'][issue] += int(count)
            else:
                filter_dict[event_tuple]['ids'] += ids
                filter_dict[event_tuple]['sources'].append(source)
                filter_dict[event_tuple]['urls'].append(url)
                if issues:
                    issue_splits = [(iss, c) for iss, c in [issue_str.split(',')
                                                            for issue_str in
                                                            issues]]
                    for issue, count in issue_splits:
                        filter_dict[event_tuple]['issues'][issue] += int(count)

    return filter_dict


def main(results):
    """
    Pulls in the coded results from PETRARCH dictionary in the
    {StoryID: [(record), (record)]} format and allows only one unique
    (DATE, SOURCE, TARGET, EVENT) tuple per day. Returns this new,
    filtered event data.

    Parameters
    ----------

    results: Dictionary.
                PETRARCH-formatted results in the
                {StoryID: [(record), (record)]} format.
    """
    logger = logging.getLogger('pipeline_log')

    logger.info('Applying one-a-day filter.')
    filtered = filter_events(results)

    return filtered
