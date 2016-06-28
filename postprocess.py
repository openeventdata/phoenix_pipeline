from __future__ import print_function
from __future__ import unicode_literals
import io
import logging
import geolocation


def create_strings(events, version):
    """
    Formats the event tuples into a string that can be written to a file.close

    Parameters
    ----------

    events: Dictionary.
                Contains filtered events. Keys are
                (DATE, SOURCE, TARGET, EVENT) tuples, values are lists of
                IDs, sources, and issues.

    version: String.
                Data version. Something like v0.1.0

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
        formatted, actors = split_process(event)
        year, month, day = formatted[:3]
        root_code, quad_class, goldstein = formatted[3:]
        story_date = event[0]
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
            lon, lat, placeName, stateName, countryName = events[event]['geo']
        else:
            lon, lat, name = '', '', ''

        actor_info = '\t'.join(actors)
        print('Event: {}\t{}\t{}\t{}\t{}'.format(story_date, actor_info, code,
                                                 ids, sources))
        id_string = '{}_{}'.format(id_count, version)
        event_str = '{}\t{}\t{}\t{}\t{}\t{}'.format(id_string, story_date,
                                                    year, month, day,
                                                    actor_info)

        event_str += '\t{}\t{}\t{}\t{}'.format(code, root_code, quad_class,
                                               goldstein)

        if joined_issues:
            event_str += '\t{}'.format(joined_issues)
        else:
            event_str += '\t'

        if lat and lon and placeName:
            event_str += '\t{}\t{}\t{}\t{}\t{}'.format(lat, lon, placeName,
                                                       stateName, countryName)
        else:
            event_str += '\t\t\t\t\t'

        event_str += '\t{}\t{}\t{}'.format(ids, urls, sources)
        event_output.append(event_str)

        id_count += 1

    event_strings = '\n'.join(event_output)

    with open('counter.txt', 'w') as f:
        f.write(str(id_count))

    return event_strings


def split_process(event):
    """
    Splits out the CAMEO code and actor information along with providing
    conversions between CAMEO codes and quad class and Goldstein values.

    Parameters
    ----------

    event: Tuple.
            (DATE, SOURCE, TARGET, EVENT) format.

    Returns
    -------

    formatted: Tuple.
                Tuple of the form
                (year, month, day, formatted_date, root_code, event_quad).

    actors: Tuple.
            Tuple containing actor information. Format is
            (source, source_root, source_agent, source_others, target,
            target_root, target_agent, target_others). Root is either
            a country code or one of IGO, NGO, IMG, or MNC. Agent is
            one of GOV, MIL, REB, OPP, PTY, COP, JUD, SPY, MED, EDU, BUS, CRM,
            or CVL. The ``others`` contains all other actor or agent codes.
    """

    year = event[0][:4]
    month = event[0][4:6]
    day = event[0][6:]
    root_code, event_quad, goldstein = process_cameo(event)

    formatted = (year, month, day, root_code, event_quad, goldstein)
    actors = process_actors(event)

    return formatted, actors


def process_cameo(event):
    """
    Provides the "root" CAMEO event, a Goldstein value for the full CAMEO code,
    and a quad class value.

    Parameters
    ----------

    event: Tuple.
            (DATE, SOURCE, TARGET, EVENT) format.

    Returns
    -------

    root_code: String.
                First two digits of a CAMEO code. Single-digit codes have
                leading zeros, hence the string format rather than

    event_quad: Int.
                    Quad class value for a root CAMEO category.

    goldstein: Float.
                Goldstein value for the full CAMEO code.
    """
    quad_conversion = {'01': 0, '02': 0, '03': 1, '04': 1, '05': 1,
                       '06': 2, '07': 2, '08': 2, '09': 3, '10': 3,
                       '11': 3, '12': 3, '13': 3, '14': 4, '15': 4,
                       '16': 3, '17': 4, '18': 4, '19': 4, '20': 4}
    #Goldstein values pulled from
    #http://eventdata.parusanalytics.com/cameo.dir/CAMEO.SCALE.txt
    goldstein_scale = {'01': 0.0, '010': 0.0, '011': -0.1, '0110': -0.1,
                       '012': -0.4, '013': 0.4, '014': 0.0, '015': 0.0,
                       '016': 3.4, '017': 0.0, '018': 3.4, '02': 3.0,
                       '020': 3.0, '021': 3.4, '0211': 3.4, '0212': 3.4,
                       '0213': 3.4, '0214': 3.4, '022': 3.4, '023': 3.4,
                       '0231': 3.4, '0232': 3.4, '0233': 3.4, '0234': 3.4,
                       '024': -0.3, '0241': -0.3, '0242': -0.3, '0243': -0.3,
                       '0244': -0.3, '025': -0.3, '0253': -0.3, '0256': -0.3,
                       '026': 4.0, '027': 4.0, '028': 4.0, '03': 4.0,
                       '030': 4.0, '031': 5.2, '0311': 5.2,
                       '0312': 5.2, '032': 4.5, '033': 5.2, '0331': 5.2,
                       '0332': 5.2, '0333': 5.2, '0334': 6.0, '034': 7.0,
                       '0341': 7.0, '0342': 7.0, '0343': 7.0, '0344': 7.0,
                       '035': 7.0, '0351': 7.0, '0352': 7.0, '0353': 7.0,
                       '0354': 7.0, '0355': 7.0, '0356': 7.0, '0357': 7.0,
                       '036': 4.0, '037': 5.0, '038': 7.0, '039': 5.0,
                       '04': 1.0, '040': 1.0, '041': 1.0, '042': 1.9,
                       '043': 2.8, '044': 2.5, '045': 5.0, '046': 7.0,
                       '05': 3.5, '050': 3.5, '051': 3.4, '052': 3.5,
                       '053': 3.8, '054': 6.0, '055': 7.0, '056': 7.0,
                       '057': 8.0, '06': 6.0, '060': 6.0, '061': 6.4,
                       '062': 7.4, '063': 7.4, '064': 7.0, '07': 7.0,
                       '070': 7.0, '071': 7.4, '072': 8.3, '073': 7.4,
                       '074': 8.5, '075': 7.0, '08': 5.0, '080': 5.0,
                       '081': 5.0, '0811': 5.0, '0812': 5.0, '0813': 5.0,
                       '0814': 5.0, '082': 5.0, '083': 5.0, '0831': 5.0,
                       '0832': 5.0, '0833': 5.0, '0834': 5.0, '084': 7.0,
                       '0841': 7.0, '0842': 7.0, '085': 7.0, '086': 9.0,
                       '0861': 9.0, '0862': 9.0, '0863': 9.0, '087': 9.0,
                       '0871': 9.0, '0872': 9.0, '0873': 9.0, '0874': 10.0,
                       '09': -2.0, '090': -2.0, '091': -2.0, '092': -2.0,
                       '093': -2.0, '094': -2.0, '10': -5.0, '100': -5.0,
                       '101': -5.0, '1014': -5.0, '102': -5.0, '103': -5.0,
                       '104': -5.0, '1041': -5.0, '1042': -5.0, '1043': -5.0,
                       '1044': -5.0, '105': -5.0, '1056': -5.0, '106': -5.0,
                       '107': -5.0, '108': -5.0, '109': -5.0, '11': -2.0,
                       '110': -2.0, '111': -2.0, '112': -2.0, '1121': -2.0,
                       '1122': -2.0, '1123': -2.0, '1124': -2.0, '1125': -2.0,
                       '113': -2.0, '114': -2.0, '115': -2.0, '12': -4.0,
                       '120': -4.0, '121': -4.0, '1211': -4.0, '1212': -4.0,
                       '1213': -4.0, '122': -4.0, '123': -4.0, '1231': -4.0,
                       '1232': -4.0, '1233': -4.0, '1234': -4.0, '124': -5.0,
                       '1246': -5.0, '125': -5.0, '126': -5.0, '127': -5.0,
                       '128': -5.0, '13': -6.0, '130': -4.4,
                       '131': -5.8, '1311': -5.8, '1312': -5.8, '1313': -5.8,
                       '132': -5.8, '1321': -5.8, '1322': -5.8, '1323': -5.8,
                       '1324': -5.8, '133': -5.8, '134': -5.8, '135': -5.8,
                       '136': -7.0, '137': -7.0, '138': -7.0, '1381': -7.0,
                       '1382': -7.0, '1383': -7.0, '1384': -7.0, '1385': -7.0,
                       '139': -7.0, '14': -6.5, '140': -6.5, '141': -6.5,
                       '1411': -6.5, '1412': -6.5, '1413': -6.5, '1414': -6.5,
                       '142': -6.5, '1421': -6.5, '1422': -6.5, '1423': -6.5,
                       '1424': -6.5, '143': -6.5, '1431': -6.5, '1432': -6.5,
                       '1433': -6.5, '1434': -6.5, '144': -7.5, '1441': -7.5,
                       '1442': -7.5, '1443': -7.5, '1444': -7.5, '145': -7.5,
                       '1451': -7.5, '1452': -7.5, '1453': -7.5, '1454': -7.5,
                       '15': -7.2, '150': -7.2, '151': -7.2, '152': -7.2,
                       '153': -7.2, '154': -7.2, '16': -4.0, '160': -4.0,
                       '161': -4.0, '162': -5.6, '1621': -5.6, '1622': -5.6,
                       '1623': -5.6, '163': -6.5, '164': -7.0, '1641': -7.0,
                       '1642': -7.0, '1643': -7.0, '165': -7.0, '166': -8.0,
                       '17': -7.0, '170': -7.0, '171': -9.2, '1711': -9.2,
                       '1712': -9.2, '172': -5.0, '1721': -5.0, '1722': -5.0,
                       '1723': -5.0, '1724': -5.0, '173': -5.0, '174': -5.0,
                       '175': -9.0, '18': -9.0, '180': -9.0, '181': -9.0,
                       '182': -9.5, '1821': -9.0, '1822': -9.0, '1823': -10.0,
                       '183': -10.0, '1831': -10.0, '1832': -10.0,
                       '1833': -10.0, '184': -8.0, '185': -8.0, '186': -10.0,
                       '19': -10.0, '190': -10.0, '191': -9.5, '192': -9.5,
                       '193': -10.0, '194': -10.0, '195': -10.0, '196': -9.5,
                       '20': -10.0, '200': -10.0, '201': -9.5, '202': -10.0,
                       '203': -10.0, '204': -10.0, '2041': -10.0,
                       '2042': -10.0}

    root_code = event[3][:2]
    try:
        event_quad = quad_conversion[root_code]
    except KeyError:
        print('Bad event: {}'.format(event))
        event_quad = ''
    try:
        goldstein = goldstein_scale[event[3]]
    except KeyError:
        print('\nMissing Goldstein Value: {}'.format(event[3]))
        try:
            goldstein = goldstein_scale[root_code]
        except KeyError:
            print('Bad event: {}'.format(event))
            goldstein = ''

    return root_code, event_quad, goldstein


def process_actors(event):
    """
    Splits out the actor codes into separate fields to enable easier
    querying/formatting of the data.

    Parameters
    ----------

    event: Tuple.
            (DATE, SOURCE, TARGET, EVENT) format.

    Returns
    -------

    actors: Tuple.
            Tuple containing actor information. Format is
            (source, source_root, source_agent, source_others, target,
            target_root, target_agent, target_others). Root is either
            a country code or one of IGO, NGO, IMG, or MNC. Agent is
            one of GOV, MIL, REB, OPP, PTY, COP, JUD, SPY, MED, EDU, BUS, CRM,
            or CVL. The ``others`` contains all other actor or agent codes.
    """
    countries = ('ABW', 'AFG', 'AGO', 'AIA', 'ALA', 'ALB', 'AND', 'ARE', 
            'ARG', 'ARM', 'ASM', 'AUT', 'AZE', 'BDI', 'BEL', 'BEN', 'BES',
            'BFA', 'BGD', 'BGR', 'BHR', 'BHS', 'BIH', 'BLM', 'BLR', 'BLZ',
            'BMU', 'BOL', 'BRA', 'BRB', 'BRN', 'BTN', 'BYS', 'BWA', 'CAF',
            'CAN', 'CHE', 'CHL', 'CHN', 'CIV', 'CMR', 'COD', 'COG', 'COK',
            'COL', 'COM', 'CPV', 'CRI', 'CSK', 'CUB', 'CUW', 'CYM', 'CYP',
            'CZE', 'DDR', 'DEU', 'DJI', 'DMA', 'DNK', 'DOM', 'DZA', 'ECU',
            'EGY', 'ERI', 'ESH', 'ESP', 'EST', 'ETH', 'FIN', 'FJI', 'FLK',
            'FRA', 'FRO', 'FSM', 'GAB', 'GBR', 'GEO', 'GGY', 'GHA', 'GIB',
            'GIN', 'GLP', 'GMB', 'GNB', 'GNQ', 'GRC', 'GRD', 'GRL', 'GTM',
            'GUF', 'GUM', 'GUY', 'HKG', 'HND', 'HRV', 'HTI', 'HUN', 'IDN',
            'IMN', 'IND', 'IRL', 'IRN', 'IRQ', 'ISL', 'ISR', 'ITA', 'JAM',
            'JEY', 'JOR', 'JPN', 'KAZ', 'KEN', 'KGZ', 'KHM', 'KIR', 'KNA',
            'KOR', 'KSV', 'KWT', 'LAO', 'LBN', 'LBR', 'LBY', 'LCA', 'LIE',
            'LKA', 'LSO', 'LTU', 'LUX', 'LVA', 'MAC', 'MAF', 'MAR', 'MCO',
            'MDA', 'MDG', 'MDV', 'MEX', 'MHL', 'MKD', 'MLI', 'MLT', 'MMR',
            'MNE', 'MNG', 'MNP', 'MOZ', 'MRT', 'MSR', 'MTQ', 'MUS', 'MWI',
            'MYS', 'MYT', 'NAM', 'NCL', 'NER', 'NFK', 'NGA', 'NIC', 'NIU',
            'NLD', 'NOR', 'NPL', 'NRU', 'NZL', 'OMN', 'PAK', 'PAN', 'PCN',
            'PER', 'PHL', 'PLW', 'PNG', 'POL', 'PRI', 'PRK', 'PRT', 'PRY',
            'PSE', 'PYF', 'QAT', 'REU', 'ROU', 'RUS', 'RWA', 'SAU', 'SCG',
            'SDN', 'SEN', 'SGP', 'SHN', 'SJM', 'SLB', 'SLE', 'SLV', 'SMR',
            'SOM', 'SPM', 'SRB', 'SSD', 'SUN', 'STP', 'SUR', 'SVK', 'SVN',
            'SWE', 'SWZ', 'SXM', 'SYC', 'SYR', 'TCA', 'TCD', 'TGO', 'THA',
            'TJK', 'TMP', 'TKL', 'TKM', 'TLS', 'TON', 'TTO', 'TUN', 'TUR',
            'TWN', 'TUV', 'TZA', 'UGA', 'UKR', 'URY', 'USA', 'UZB', 'VAT',
            'VCT', 'VEN', 'VGB', 'VIR', 'VNM', 'VUT', 'WLF', 'WSM', 'YEM',
            'YMD', 'YUG', 'ZAF', 'ZAR', 'ZMB', 'ZWE', 'ATG', 'AUS')
    root_actors = ('IGO', 'NGO', 'IMG', 'MNC')
    primary_agent = ('GOV', 'MIL', 'REB', 'OPP', 'PTY', 'COP', 'JUD', 'SPY',
                     'MED', 'EDU', 'BUS', 'CRM', 'CVL')

    sauce = event[1]
    targ = event[2]

    if sauce[:3] in countries or sauce[:3] in root_actors:
        sauce_root = sauce[:3]
    else:
        sauce_root = ''

    if sauce[3:6] in primary_agent:
        sauce_agent = sauce[3:6]
    else:
        sauce_agent = ''

    sauce_others = ''
    if len(sauce) > 3:
        if sauce_agent:
            start = 6
            length = len(sauce[6:])
        else:
            start = 3
            length = len(sauce[3:])

        for i in range(start, start + length, 3):
            sauce_others += sauce[i:i + 3] + ';'
        sauce_others = sauce_others[:-1]

    if targ[:3] in countries or targ[:3] in root_actors:
        targ_root = targ[:3]
    else:
        targ_root = ''

    if targ[3:6] in primary_agent:
        targ_agent = targ[3:6]
    else:
        targ_agent = ''

    targ_others = ''
    if len(targ) > 3:
        if targ_agent:
            start = 6
            length = len(targ[6:])
        else:
            start = 3
            length = len(targ[3:])

        for i in range(start, start + length, 3):
            targ_others += targ[i:i + 3] + ';'
        targ_others = targ_others[:-1]

    actors = (sauce, sauce_root, sauce_agent, sauce_others, targ, targ_root,
              targ_agent, targ_others)
    return actors


def main(event_dict, this_date, version, file_details, server_details, geo_details):
    """
    Pulls in the coded results from PETRARCH dictionary in the
    {StoryID: [(record), (record)]} format and allows only one unique
    (DATE, SOURCE, TARGET, EVENT) tuple per day. Returns this new,
    filtered event data.

    Parameters
    ----------

    event_dict: Dictionary.
                PETRARCH-formatted results in the
                {StoryID: [(record), (record)]} format.

    this_date: String.
                The current date the pipeline is running.

    version: String.
                Data version. Something like v0.1.0

    file_details: NamedTuple.
                    Container generated from the config file specifying file
                    stems and other relevant options.
    server_details: NamedTuple.
                    Info for uploading to server.
    geo_details: NamedTuple.
                    Info about geo type and geo server details.
    """
    logger = logging.getLogger('pipeline_log')

    logger.info('Geolocating.')
    print('Geolocating')
    geo_details.geo_service == "Mordecai"
    print(event_dict)
    if geo_details.geo_service == "CLIFF":
        updated_events = geolocation.cliff(event_dict, file_details, server_details, geo_details)
    elif geo_details.geo_service == "Mordecai":
        updated_events = geolocation.mordecai(event_dict, file_details, server_details, geo_details)
    else:
        print("Invalid geo service name. Must be 'CLIFF' or 'Mordecai'.")

    logger.info('Formatting events for output.')
    event_write = create_strings(updated_events, version)

    logger.info('Writing event output.')
    filename = '{}{}.txt'.format(file_details.fullfile_stem, this_date)
    with io.open(filename, 'w', encoding='utf-8') as f:
        f.write(event_write)

    print("Finished")
