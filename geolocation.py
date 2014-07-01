from __future__ import unicode_literals
from __future__ import print_function
import json
import requests
import utilities
from bson.objectid import ObjectId


def query_geotext(sentence):
    """
    Filters out duplicate events, leaving only one unique
    (DATE, SOURCE, TARGET, EVENT) tuple per day.


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
    """
    q = "http://geotxt.org/api/1/geotxt.json?m=stanfords&q={}".format(sentence)

    query_out = requests.get(q)
    try:
        geo_results = json.loads(query_out.content)
    except Exception as e:
        print('There was an error: {}. Status code: {}'.format(e,
                                                               query_out.status_code))
        geo_results = {'features': []}

    if geo_results['features']:
        try:
            lon, lat = geo_results['features'][0]['geometry']['coordinates']
            name = geo_results['features'][0]['properties']['toponym']
        except Exception as e:
            lon, lat, name = '', '', ''
    else:
        lon, lat, name = '', '', ''

    return lon, lat, name


def main(events, file_details):
    """
    Pulls out a database ID and runs the ``query_geotext`` function to hit the
    GeoVista Center's GeoText API and find location information within the
    sentence.

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
            a tuple of the form (LAT, LON).
    """
    conn = utilities.make_conn(file_details.auth_db, file_details.auth_user,
                               file_details.auth_pass)

    for event in events:
        event_id, sentence_id = events[event]['ids'][0].split('_')
        result = coll.find_one({'_id': ObjectId(event_id.split('_')[0])})
        sents = utilities.sentence_segmenter(result['content'])

        query_text = sents[int(sentence_id)]
        lon, lat, name = query_geotext(query_text)
        if lat and lon:
            events[event]['geo'] = (lon, lat, name)

    return events


if __name__ == '__main__':
    print('Not designed to be run as a stand-alone script.')
