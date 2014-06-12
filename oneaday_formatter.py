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
        date = story[0]
        source = story[1]
        target = story[2]
        code = story[3]
        if len(story) == 7:
            ids = story[4].split(';')
            url = story[5]
            source = story[6]
            issues = ''
        else:
            issues = story[4]
            issues = issues.split(';')
            ids = story[5].split(';')
            url = story[6]
            source = story[7]

        event_tuple = (date, source, target, code)

        if event_tuple not in filter_dict:
            filter_dict[event_tuple] = {'issues': Counter(), 'ids': ids,
                                        'sources': [source], 'urls': [url]}
            if issues:
                print issues
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

    for event in events:
        story_date = event[0]
        source = event[1]
        target = event[2]
        code = event[3]

        ids = ';'.join(events[event]['ids'])
        sources = ';'.join(events[event]['sources'])
        urls = ';'.join(events[event]['urls'])

        if 'issues' in events[event]:
            iss = events[event]['issues']
            issues = ['{},{}'.format(k, v) for k, v in iss.iteritems()]
            joined_issues = ';'.join(issues)
        else:
            joined_issues = []

        print 'Event: {}\t{}\t{}\t{}\t{}\t{}'.format(story_date, source,
                                                     target, code, ids,
                                                     sources)
        event_str = '{}\t{}\t{}\t{}'.format(story_date,
                                            source,
                                            target,
                                            code)
        if joined_issues:
            event_str += '\t{}'.format(joined_issues)

        event_str += '\t{}\t{}\t{}'.format(ids, urls, sources)
        event_output.append(event_str)

    event_strings = '\n'.join(event_output)
    return event_strings


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
    event_write = create_strings(filtered)

    logger.info('Writing event output.')
    filename = '{}{}.txt'.format(file_details.fullfile_stem, this_date)
    with open(filename, 'w') as f:
        f.write(event_write)

    print "Finished"
