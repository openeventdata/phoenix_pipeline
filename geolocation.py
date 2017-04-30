from __future__ import unicode_literals        
from __future__ import print_function        
import logging        
import requests        
import utilities        
from bson.objectid import ObjectId
import json

def query_cliff(sentence, host, port):
    """
    Takes a sentence from a news article, passes it to the CLIFF geolocation
    service, and extracts the relevant data that CLIFF returns.
    Parameters
    ----------
    sentence: String.
                Text from which an event was coded.
    Returns
    -------
    lat: String.
            Latitude of a location.
    lon: String.
            Longitude of a location.
    placeName: String.
            The name of the most precise location extracted from the sentence.
    stateName: String.
            The name of the state/region/province extracted from the sentence.
    countryCode: String.
            The ISO 3 character country code of the country  extracted from the sentence.
    """
    logger = logging.getLogger('pipeline_log')

    payload = {"q": sentence}

    place_info = {'lat': '', 'lon': '', 'placeName': '', 'countryCode': '',
                  'stateName': '', 'restype' : ''}

    cliff_address = "{}:{}/CLIFF-2.0.0/parse/text".format(host, port)
    try:
        located = requests.get(cliff_address, params=payload).json()

    except Exception as e:
        logger.warning('There was an error requesting geolocation. {}'.format(e))
        return place_info

    try:
        focus = located['results']['places']['focus']
    except:
        return place_info
   # logger.warning(focus)

    if not focus:
        return place_info
 # If there's a city, we want that.
    if focus['cities']:
        # If there's more than one city, we just want the first.
        # (That's questionable, but eh).
        if len(focus['cities']) > 1:
            try:
                lat = focus['cities'][0]['lat']
                lon = focus['cities'][0]['lon']
                placeName = focus['cities'][0]['name']
                countryCode = focus['cities'][0]['countryCode']
                #countryDetails = focus['countries']
                #for deet in countryDetails:
                #    if deet['countryCode'] == countryCode:
                #        countryName = deet['name']
                #    else:
                #        countryName = ''  # shouldn't need these...
                stateCode = focus['cities'][0]['countryCode']
                stateDetails = focus['states']
                for deet in stateDetails:
                    if deet['stateCode'] == stateCode:
                        stateName = deet['name']
                    else:
                        stateName = ''
                place_info = {'lat': lat, 'lon': lon, 'placeName': placeName,
                              'restype': 'city', 'countryCode': countryCode,
                              'stateName': stateName}
                return place_info
            except:
                logger.warning("Error on story. (multiple cities)")
                logger.info(sentence)
                return place_info
        # If there's only one city, we're good to go.
        elif len(focus['cities']) == 1:
            try:
                lat = focus['cities'][0]['lat']
                lon = focus['cities'][0]['lon']
                placeName = focus['cities'][0]['name']
                countryCode = focus['cities'][0]['countryCode']
                #countryDetails = focus['countries']
                #for deet in countryDetails:
                #    if deet['countryCode'] == countryCode:
                #        countryName = deet['name']
                #    else:
                #        countryName = ''
                stateCode = focus['cities'][0]['stateCode']
                stateDetails = focus['states']
                for deet in stateDetails:
                    if deet['stateCode'] == stateCode:
                        stateName = deet['name']
                    else:
                        stateName = ''
                place_info = {'lat': lat, 'lon': lon, 'placeName': placeName,
                              'restype': 'city', 'countryCode': countryCode,
                              'stateName': stateName}
                return place_info
            except:
                logger.warning("Error on story. (city == 1).")
                logger.info(sentence)
                return place_info
    # If there's no city, we'll take a state.
    elif (len(focus['states']) > 0) & (len(focus['cities']) == 0):
        #if len(focus['states']) > 1:
        #    stateslist = focus['states'][0]
        #    lat = stateslist['states']['lat']
        #    lon = stateslist['states']['lon']
        #    placename = statelist['states']['name']
        if len(focus['states']) == 1:
            try:
                lat = focus['states'][0]['lat']
                lon = focus['states'][0]['lon']
                placeName = focus['states'][0]['name']
                countryCode = focus['states'][0]['countryCode']
                #countryDetails = focus['countries']
                #for deet in countryDetails:
                #    if deet['countryCode'] == countryCode:
                #        countryName = deet['name']
                #    else:
                #        countryName = ''
                place_info = {'lat': lat, 'lon': lon, 'placeName': placeName,
                              'restype': 'state', 'countryCode': countryCode,
                              'stateName': stateName}
                return place_info
            except:
                logger.warning("""Error on story. (cities == 0, states !=
                        0)""")
                logger.info(sentence)
                return place_info
    #if ((focus['cities'] == []) & len(focus['states']) > 0):
       # lat = focus['cities']['lat']
       # lon = focus['cities']['lon']
       # placename = focus['cities']['name']
    elif (len(focus['cities']) == 0) & (len(focus['states']) == 0):
        try:
            lat = focus['countries'][0]['lat']
            lon = focus['countries'][0]['lon']
            countryCode = focus['countries'][0]['countryCode']
            placeName = focus['countries'][0]['name']
            place_info = {'lat': lat, 'lon': lon, 'placeName': placeName,
                          'restype': 'country', 'countryCode': countryCode,
                          'stateName': ''}
            return place_info
        except:
            logger.warning("""Error on story. (cities == 0, states == 0)""")
            logger.info(sentence)
            return place_info

def iso_convert(iso2c):
    """
    Takes a two character ISO country code and returns the corresponding 3
    character ISO country code.

    Parameters
    ----------

    iso2c: A two character ISO country code.

    Returns
    -------
    iso3c: A three character ISO country code.
    """

    iso_dict = {"AD":"AND", "AE":"ARE", "AF":"AFG", "AG":"ATG", "AI":"AIA",
                "AL":"ALB", "AM":"ARM", "AO":"AGO", "AQ":"ATA", "AR":"ARG",
                "AS":"ASM", "AT":"AUT", "AU":"AUS", "AW":"ABW", "AX":"ALA",
                "AZ":"AZE", "BA":"BIH", "BB":"BRB", "BD":"BGD", "BE":"BEL",
                "BF":"BFA", "BG":"BGR", "BH":"BHR", "BI":"BDI", "BJ":"BEN",
                "BL":"BLM", "BM":"BMU", "BN":"BRN", "BO":"BOL", "BQ":"BES",
                "BR":"BRA", "BS":"BHS", "BT":"BTN", "BV":"BVT", "BW":"BWA",
                "BY":"BLR", "BZ":"BLZ", "CA":"CAN", "CC":"CCK", "CD":"COD",
                "CF":"CAF", "CG":"COG", "CH":"CHE", "CI":"CIV", "CK":"COK",
                "CL":"CHL", "CM":"CMR", "CN":"CHN", "CO":"COL", "CR":"CRI",
                "CU":"CUB", "CV":"CPV", "CW":"CUW", "CX":"CXR", "CY":"CYP",
                "CZ":"CZE", "DE":"DEU", "DJ":"DJI", "DK":"DNK", "DM":"DMA",
                "DO":"DOM", "DZ":"DZA", "EC":"ECU", "EE":"EST", "EG":"EGY",
                "EH":"ESH", "ER":"ERI", "ES":"ESP", "ET":"ETH", "FI":"FIN",
                "FJ":"FJI", "FK":"FLK", "FM":"FSM", "FO":"FRO", "FR":"FRA",
                "GA":"GAB", "GB":"GBR", "GD":"GRD", "GE":"GEO", "GF":"GUF",
                "GG":"GGY", "GH":"GHA", "GI":"GIB", "GL":"GRL", "GM":"GMB",
                "GN":"GIN", "GP":"GLP", "GQ":"GNQ", "GR":"GRC", "GS":"SGS",
                "GT":"GTM", "GU":"GUM", "GW":"GNB", "GY":"GUY", "HK":"HKG",
                "HM":"HMD", "HN":"HND", "HR":"HRV", "HT":"HTI", "HU":"HUN",
                "ID":"IDN", "IE":"IRL", "IL":"ISR", "IM":"IMN", "IN":"IND",
                "IO":"IOT", "IQ":"IRQ", "IR":"IRN", "IS":"ISL", "IT":"ITA",
                "JE":"JEY", "JM":"JAM", "JO":"JOR", "JP":"JPN", "KE":"KEN",
                "KG":"KGZ", "KH":"KHM", "KI":"KIR", "KM":"COM", "KN":"KNA",
                "KP":"PRK", "KR":"KOR", "XK":"XKX", "KW":"KWT", "KY":"CYM",
                "KZ":"KAZ", "LA":"LAO", "LB":"LBN", "LC":"LCA", "LI":"LIE",
                "LK":"LKA", "LR":"LBR", "LS":"LSO", "LT":"LTU", "LU":"LUX",
                "LV":"LVA", "LY":"LBY", "MA":"MAR", "MC":"MCO", "MD":"MDA",
                "ME":"MNE", "MF":"MAF", "MG":"MDG", "MH":"MHL", "MK":"MKD",
                "ML":"MLI", "MM":"MMR", "MN":"MNG", "MO":"MAC", "MP":"MNP",
                "MQ":"MTQ", "MR":"MRT", "MS":"MSR", "MT":"MLT", "MU":"MUS",
                "MV":"MDV", "MW":"MWI", "MX":"MEX", "MY":"MYS", "MZ":"MOZ",
                "NA":"NAM", "NC":"NCL", "NE":"NER", "NF":"NFK", "NG":"NGA",
                "NI":"NIC", "NL":"NLD", "NO":"NOR", "NP":"NPL", "NR":"NRU",
                "NU":"NIU", "NZ":"NZL", "OM":"OMN", "PA":"PAN", "PE":"PER",
                "PF":"PYF", "PG":"PNG", "PH":"PHL", "PK":"PAK", "PL":"POL",
                "PM":"SPM", "PN":"PCN", "PR":"PRI", "PS":"PSE", "PT":"PRT",
                "PW":"PLW", "PY":"PRY", "QA":"QAT", "RE":"REU", "RO":"ROU",
                "RS":"SRB", "RU":"RUS", "RW":"RWA", "SA":"SAU", "SB":"SLB",
                "SC":"SYC", "SD":"SDN", "SS":"SSD", "SE":"SWE", "SG":"SGP",
                "SH":"SHN", "SI":"SVN", "SJ":"SJM", "SK":"SVK", "SL":"SLE",
                "SM":"SMR", "SN":"SEN", "SO":"SOM", "SR":"SUR", "ST":"STP",
                "SV":"SLV", "SX":"SXM", "SY":"SYR", "SZ":"SWZ", "TC":"TCA",
                "TD":"TCD", "TF":"ATF", "TG":"TGO", "TH":"THA", "TJ":"TJK",
                "TK":"TKL", "TL":"TLS", "TM":"TKM", "TN":"TUN", "TO":"TON",
                "TR":"TUR", "TT":"TTO", "TV":"TUV", "TW":"TWN", "TZ":"TZA",
                "UA":"UKR", "UG":"UGA", "UM":"UMI", "US":"USA", "UY":"URY",
                "UZ":"UZB", "VA":"VAT", "VC":"VCT", "VE":"VEN", "VG":"VGB",
                "VI":"VIR", "VN":"VNM", "VU":"VUT", "WF":"WLF", "WS":"WSM",
                "YE":"YEM", "YT":"MYT", "ZA":"ZAF", "ZM":"ZMB", "ZW":"ZWE",
                "CS":"SCG", "AN":"ANT"}

    try:
        iso3c = iso_dict[iso2c]
        return iso3c
    except KeyError:
        print('Bad code: ' + iso2c)
        iso3c = "NA"
        return iso3c

def query_mordecai(sentence, host, port):
    """
    Takes a sentence from a news article, passes it to the Mordecai geolocation
    service, and extracts the relevant data that Mordecai returns.
    Parameters
    ----------
    sentence: String.
                Text from which an event was coded.
    host: String
               Host where Mordecai is running (taken from config)
    port: String
               Port that Mordecai service is listening on 
    Returns
    -------
    lat: String.
            Latitude of a location.
    lon: String.
            Longitude of a location.
    placeName: String.
            The name of the most precise location extracted from the sentence.
    stateName: String.
            The name of the state/region/province extracted from the sentence.
    countryCode: String.
            The ISO 3 character country code of the country  extracted from the sentence.
    """
    headers = {'Content-Type': 'application/json'}
    data = {'text': sentence}
    data = json.dumps(data)
    dest = "{0}:{1}/places".format(host, port)
    out = requests.post(dest, data=data, headers=headers)
    return json.loads(out.text)

def test_mordecai(sentence, host, port):
    """
    Check if Mordecai service is up and responding on given host and port.
    Parameters
    ----------
    sentence: String.
                Text from which an event was coded.
    """

def mordecai(events, file_details, server_details, geo_details):
    """
    Pulls out a database ID and queries the Mordecai geolocation system 
    running locally  and find location information within the sentence.
    Parameters
    ----------
    events: Dictionary.
            Contains filtered events from the one-a-day filter. Keys are
            (DATE, SOURCE, TARGET, EVENT) tuples, values are lists of
            IDs, sources, and issues.
    Returns
    -------
    events: Dictionary.
            Same as in the parameter but with the addition of a value that is
            a list of lon, lat, placeName, stateName, countryCode.
    """
    coll = utilities.make_conn(file_details.db_db, file_details.db_collection,
                               file_details.auth_db, file_details.auth_user,
                               file_details.auth_pass)

    for event in events:
        event_id, sentence_id = events[event]['ids'][0].split('_')
       # print(event_id)
        result = coll.find_one({'_id': ObjectId(event_id.split('_')[0])})
        sents = utilities.sentence_segmenter(result['content'])

        query_text = sents[int(sentence_id)]
        geo_info = query_mordecai(query_text, geo_details.mordecai_host,
                               geo_details.mordecai_port)
        try:
            # temporary hack: take the first location:
            geo_info = geo_info[0]
            # NA is for ADM1, which mord doesn't return. See issue #2
            events[event]['geo'] = (geo_info['lon'], geo_info['lat'],
                              geo_info['placename'], "NA", geo_info['countrycode'])
        except Exception as e:
            events[event]['geo'] = ("NA", "NA", "NA", "NA", "NA")

    return events

def cliff(events, file_details, server_details, geo_details):
    """
    Pulls out a database ID and runs the ``query_cliff`` function to hit MIT's
    CLIFF/CLAVIN geolocation system running locally and find location
    information within the sentence. Note, this function calls back to the database
    where stories are stored.
    Parameters
    ----------
    events: Dictionary.
            Contains filtered events from the one-a-day filter. Keys are
            (DATE, SOURCE, TARGET, EVENT) tuples, values are lists of
            IDs, sources, and issues.
    Returns
    -------
    events: Dictionary.
            Same as in the parameter but with the addition of a value that is
            a list of lon, lat, placeName, stateName, countryCode.
    """
    coll = utilities.make_conn(file_details.db_db, file_details.db_collection,
                               file_details.auth_db, file_details.auth_user,
                               file_details.auth_pass)

    for event in events:
        event_id, sentence_id = events[event]['ids'][0].split('_')
       # print(event_id)
        result = coll.find_one({'_id': ObjectId(event_id.split('_')[0])})
        sents = utilities.sentence_segmenter(result['content'])

        query_text = sents[int(sentence_id)]
        geo_info = query_cliff(query_text, geo_details.cliff_host,
                               geo_details.cliff_port)
        if geo_info:
            try:
                if geo_info['countryCode'] != "":
                    geo_info['countryCode'] = iso_convert(geo_info['countryCode'])
            except:
                logger.warning("""Error converting country codes.""")
            events[event]['geo'] = (geo_info['lon'], geo_info['lat'],
                                    geo_info['placeName'],
                                    geo_info['stateName'],
                                    geo_info['countryCode'])
            # Add in country and restype here
    return events
