from bson.objectid import ObjectId
import datetime
import sys
import os
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../")
import geolocation
import utilities

def test_geo_config():
    server_details, geo_details, file_details, petrarch_version = utilities.parse_config('PHOX_config.ini')
    geo_keys = geo_details._asdict().keys()
    assert geo_keys == ['geo_service', 'cliff_host', 'cliff_port', 'mordecai_host', 'mordecai_port']

