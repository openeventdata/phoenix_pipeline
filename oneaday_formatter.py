from __future__ import print_function
from __future__ import unicode_literals
import io
import logging
import geolocation
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


def create_strings(events):
    """
    Formats the event tuples into a string that can be written to a file.close

    Parameters
    ----------

    events: Dictionary.
                Contains filtered events. Keys are
                (DATE, SOURCE, TARGET, EVENT) tuples, values are lists of
                IDs, sources, and issues.

    Returns
    -------

    event_strings: String.
                    Contains tab-separated event entries with \n as a line
                    delimiter.
    """
    event_output = []
    with open('counter.txt', 'r') as f:
        id_count = int(f.read().replace('\n', ''))

    for event in events:
        formatted = split_process(event)
        formatted_date, year, month, day = formatted[:4]
        root_code, quad_class = formatted[4:]
        story_date = event[0]
        src = event[1]
        target = event[2]
        code = event[3]

        ids = ';'.join(events[event]['ids'])
        sources = ';'.join(events[event]['sources'])
        urls = ';'.join(events[event]['urls'])

        if 'issues' in events[event]:
            iss = events[event]['issues']
            issues = ['{},{}'.format(k, v) for k, v in iss.items()]
            joined_issues = ';'.join(issues)
        else:
            joined_issues = []

        if 'geo' in events[event]:
            lon, lat, name = events[event]['geo']
        else:
            lon, lat, name = '', '', ''

        print('Event: {}\t{}\t{}\t{}\t{}\t{}'.format(story_date, src,
                                                     target, code, ids,
                                                     sources))
        event_str = '{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}'.format(id_count,
                                                            story_date,
                                                            formatted_date,
                                                            year, month, day,
                                                            src, target)

        event_str += '\t{}\t{}\t{}'.format(code, root_code, quad_class)

        if joined_issues:
            event_str += '\t{}'.format(joined_issues)
        else:
            event_str += '\t'

        if lat and lon and name:
            event_str += '\t{}\t{}\t{}'.format(lat, lon, name)
        else:
            event_str += '\t\t\t'

        event_str += '\t{}\t{}\t{}'.format(ids, urls, sources)
        event_output.append(event_str)

        id_count += 1

    event_strings = '\n'.join(event_output)

    with open('counter.txt', 'w') as f:
        f.write(id_count)

    return event_strings


def split_process(event):
    """
    Splits out the CAMEO code, provides a conversion to the quad class,
    provides a formatted date.

    Parameters
    ----------

    event: Tuple.
            (DATE, SOURCE, TARGET, EVENT) format.

    Returns
    -------

    formatted: Tuple.
                Tuple of the form
                (year, month, day, formatted_date, root_code, event_quad).
    """

    year = event[0][:4]
    month = event[0][4:6]
    day = event[0][6:]
    formatted_date = '{}-{}-{}'.format(year, month, day)
    quad_conversion = {'01': '0', '02': '0', '03': '1', '04': '1', '05': '1',
                       '06': '2', '07': '2', '08': '2', '09': '3', '10': '3',
                       '11': '3', '12': '3', '13': '3', '14': '4', '15': '4',
                       '16': '3', '17': '4', '18': '4', '19': '4', '20': '4'}
    root_code = event[3][:2]
    event_quad = quad_conversion[root_code]

    formatted = (formatted_date, year, month, day, root_code, event_quad)

    return formatted


def main(results, this_date, server_list, file_details):
    """
    Pulls in the coded results from PETRARCH dictionary in the
    {StoryID: [(record), (record)]} format and allows only one unique
    (DATE, SOURCE, TARGET, EVENT) tuple per day. Writes out this new,
    filtered event data.

    Parameters
    ----------

    results: Dictionary.
                PETRARCH-formatted results in the
                {StoryID: [(record), (record)]} format.

    file_details: NamedTuple.
                    Container generated from the config file specifying file
                    stems and other relevant options.

    this_date: String.
                The current date the pipeline is running.
    """
    logger = logging.getLogger('pipeline_log')

    logger.info('Applying one-a-day filter.')
    filtered = filter_events(results)
    updated_events = geolocation.main(filtered)
    event_write = create_strings(updated_events)

    logger.info('Writing event output.')
    filename = '{}{}.txt'.format(file_details.fullfile_stem, this_date)
    with io.open(filename, 'w', encoding='utf-8') as f:
        f.write(event_write)

    print("Finished")
