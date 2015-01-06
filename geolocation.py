from __future__ import unicode_literals
from __future__ import print_function
import requests
import utilities
from bson.objectid import ObjectId


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

    countryName: String.
            The name of the country extracted from the sentence.
    """

    payload = {"q": sentence}

    place_info = {'lat': '', 'lon': '', 'placeName': '', 'countryName': '',
                  'stateName': ''}

    cliff_address = "http://{}:{}/CLIFF-2.0.0/parse/text".format(host, port)
    try:
        located = requests.get(cliff_address, params=payload).json()

    except Exception as e:
        print('There was an error requesting geolocation. {}'.format(e))
        return place_info

    focus = located['results']['places']['focus']
   # print(focus)

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
                countryDetails = focus['countries']
                for deet in countryDetails:
                    if deet['countryCode'] == countryCode:
                        countryName = deet['name']
                    else:
                        countryName = ''  # shouldn't need these...
                stateCode = focus['cities'][0]['countryCode']
                stateDetails = focus['states']
                for deet in stateDetails:
                    if deet['stateCode'] == stateCode:
                        stateName = deet['name']
                    else:
                        stateName = ''
                place_info = {'lat': lat, 'lon': lon, 'placeName': placeName,
                              'restype': 'state', 'countryName': countryName,
                              'stateName': stateName}
                return place_info
            except:
                print("Error on story. It thought there were multiple cities")
                print(sentence)
                return place_info
        # If there's only one city, we're good to go.
        elif len(focus['cities']) == 1:
            try:
                lat = focus['cities'][0]['lat']
                lon = focus['cities'][0]['lon']
                placeName = focus['cities'][0]['name']
                countryCode = focus['cities'][0]['countryCode']
                countryDetails = focus['countries']
                for deet in countryDetails:
                    if deet['countryCode'] == countryCode:
                        countryName = deet['name']
                    else:
                        countryName = ''
                stateCode = focus['cities'][0]['stateCode']
                stateDetails = focus['states']
                for deet in stateDetails:
                    if deet['stateCode'] == stateCode:
                        stateName = deet['name']
                    else:
                        stateName = ''
                place_info = {'lat': lat, 'lon': lon, 'placeName': placeName,
                              'restype': 'state', 'countryName': countryName,
                              'stateName': stateName}
                return place_info
            except:
                print("Error on story. It thought there was 1 city.")
                print(sentence)
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
                countryDetails = focus['countries']
                for deet in countryDetails:
                    if deet['countryCode'] == countryCode:
                        countryName = deet['name']
                    else:
                        countryName = ''
                place_info = {'lat': lat, 'lon': lon, 'placeName': placeName,
                              'restype': 'state', 'countryName': countryName,
                              'stateName': stateName}
                return place_info
            except:
                print("""Error on story. It thought there were no cities but
                        1 state.""")
                print(sentence)
                return place_info
    #if ((focus['cities'] == []) & len(focus['states']) > 0):
       # lat = focus['cities']['lat']
       # lon = focus['cities']['lon']
       # placename = focus['cities']['name']
    elif (len(focus['cities']) == 0) & (len(focus['states']) == 0):
        try:
            lat = focus['countries'][0]['lat']
            lon = focus['countries'][0]['lon']
            placeName = focus['countries'][0]['name']
            place_info = {'lat': lat, 'lon': lon, 'placeName': placeName,
                          'restype': 'country', 'countryName': placeName,
                          'stateName': ''}
            return place_info
        except:
            print("""Error on story. It thought there were no cities or
                    states--going for country""")
            print(sentence)
            return place_info


def main(events, file_details, server_details):
    """
    Pulls out a database ID and runs the ``query_cliff`` function to hit MIT's
    CLIFF/CLAVIN geolocation system running locally  and find location
    information within the sentence.

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
            a list of lon, lat, placeName, stateName, countryName.
    """
    coll = utilities.make_conn(file_details.auth_db, file_details.auth_user,
                               file_details.auth_pass)

    for event in events:
        event_id, sentence_id = events[event]['ids'][0].split('_')
       # print(event_id)
        result = coll.find_one({'_id': ObjectId(event_id.split('_')[0])})
        sents = utilities.sentence_segmenter(result['content'])

        query_text = sents[int(sentence_id)]
        geo_info = query_cliff(query_text, server_details.cliff_host,
                               server_details.cliff_port)
        if geo_info:
            events[event]['geo'] = (geo_info['lon'], geo_info['lat'],
                                    geo_info['placeName'],
                                    geo_info['stateName'],
                                    geo_info['countryName'])
            # Add in country and restype here
    return events
