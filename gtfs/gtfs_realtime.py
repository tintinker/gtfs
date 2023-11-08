#!/usr/bin/python

# gtfsrdb.py: load gtfs-realtime data to a database
# recommended to have the (static) GTFS data for the agency you are connecting
# to already loaded.

# Copyright 2011, 2013 Matt Conway

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#   http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Authors:
# Matt Conway: main code
# Jorge Adorno


from datetime import datetime, timedelta
import time
import sys
from optparse import OptionParser
import logging
from urllib.request import urlopen, Request
from urllib.parse import urlencode, urlparse, parse_qs
import logging


from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from google.transit import gtfs_realtime_pb2
from model import *

p = OptionParser()

p.add_option('-u', '--trip-updates', dest='tripUpdates', default=None,
             help='The trip updates URL', metavar='URL')


p.add_option('-d', '--database', default=None, dest='dsn',
             help='Database connection string', metavar='DSN')


p.add_option('-c', '--create-tables', default=False, dest='create',
             action='store_true', help="Create tables if they aren't found")


p.add_option('-w', '--wait', default=30, type='int', metavar='SECS',
             dest='timeout', help='Time to wait between requests (in seconds)')

p.add_option('-k', '--kill-after', default=0, dest='killAfter', type="float",
             help='Kill process after this many minutes')

p.add_option('-v', '--verbose', default=True, dest='verbose',
             action='store_false', help='Print generated SQL')

p.add_option('-l', '--language', default='en', dest='lang', metavar='LANG',
             help='When multiple translations are available, prefer this language')

p.add_option('-x', '--apikey', dest='apiKey', default=None,
             help='The API key, if required', metavar='API_KEY')

p.add_option('-t', '--timezone', default=None, type=int, dest='tz',
            help='UTC Time Zone Offset (ex. -8 for Pacific Standard Time)', metavar='TZ')

p.add_option('-f', '--logfile', default=None, dest='logfile',
            help='log file', metavar='LOGFILE')

opts, args = p.parse_args()


# Set up a logger
logging.basicConfig(filename=opts.logfile, encoding='utf-8', level=logging.DEBUG)
logger = logging.getLogger()

if opts.dsn is None:
    logging.error('No database specified!')
    exit(1)

if opts.tripUpdates is None:
    logging.error('No trip updates URL was specified!')
    exit(1)

if opts.tz is None:
    logging.error('No time zone specified!')
    exit(1)

# Connect to the database
engine = create_engine(opts.dsn, echo=opts.verbose)

# Create a database inspector
insp = inspect(engine)
# sessionmaker returns a class
session = sessionmaker(bind=engine)()

# Check if it has the tables
# Base from model.py
for table in Base.metadata.tables.keys():
    if not insp.has_table(table):
        if opts.create:
            logging.info('Creating table %s', table)
            Base.metadata.tables[table].create(engine)
        else:
            logging.error('Missing table %s! Use -c to create it.', table)
            exit(1)

def getTrans(string, lang):
    '''Get a specific translation from a TranslatedString.'''
    # If we don't find the requested language, return this
    untranslated = None

    # single translation, return it
    if len(string.translation) == 1:
        return string.translation[0].text

    for t in string.translation:
        if t.language == lang:
            return t.text
        if t.language is None:
            untranslated = t.text
    return untranslated

def seconds_since_midnight(dt):
    midnight = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    delta = dt - midnight
    return delta.total_seconds()

if opts.killAfter > 0:
    stop_time = datetime.now() + timedelta(minutes=opts.killAfter)

def format_req(url):
    if not opts.apiKey:
        return Request(url)
    
    parsed_url = urlparse(url)
    query_parameters = {'api_key': opts.apiKey, 'token': opts.apiKey }
    existing_query_params = parse_qs(parsed_url.query)
    existing_query_params.update(query_parameters)
    new_query = urlencode(existing_query_params, doseq=True)
    new_url = parsed_url._replace(query=new_query).geturl()
    
    req = Request(new_url)
    req.add_header('Authorization', opts.apiKey)
    
    return req
    

try:
    keep_running = True
    while keep_running:
        if opts.killAfter > 0:
            if datetime.now() > stop_time:
                sys.exit()
        try:
            if opts.tripUpdates:
                fm = gtfs_realtime_pb2.FeedMessage()
                fm.ParseFromString(
                    urlopen(format_req(opts.tripUpdates)).read()
                )

                # Convert this a Python object, and save it to be placed into each
                # trip_update
                timestamp = datetime.utcfromtimestamp(fm.header.timestamp)

                # Check the feed version
                if fm.header.gtfs_realtime_version != u'1.0':
                    logging.warning('Warning: feed version has changed: found %s, expected 1.0', fm.header.gtfs_realtime_version)

                logging.info('Adding %s trip updates', len(fm.entity))
                for entity in fm.entity:

                    tu = entity.trip_update

                    dbtu = TripUpdate(
                        trip_id=tu.trip.trip_id,
                        route_id=tu.trip.route_id,
                        trip_start_time=tu.trip.start_time,
                        trip_start_date=tu.trip.start_date,

                        # get the schedule relationship
                        # This is somewhat undocumented, but by referencing the
                        # DESCRIPTOR.enum_types_by_name, you get a dict of enum types
                        # as described at
                        # http://code.google.com/apis/protocolbuffers/docs/reference/python/google.protobuf.descriptor.EnumDescriptor-class.html
                        schedule_relationship=tu.trip.DESCRIPTOR.enum_types_by_name[
                            'ScheduleRelationship'].values_by_number[tu.trip.schedule_relationship].name,

                        vehicle_id=tu.vehicle.id,
                        vehicle_label=tu.vehicle.label,
                        vehicle_license_plate=tu.vehicle.license_plate,
                        timestamp=timestamp)

                    for stu in tu.stop_time_update:
                        local_arrival_time = datetime.utcfromtimestamp(stu.arrival.time) + timedelta(hours = opts.tz)
                        since_midnight = seconds_since_midnight(local_arrival_time)
                        dbstu = StopTimeUpdate(
                            stop_sequence=stu.stop_sequence,
                            stop_id=stu.stop_id,
                            arrival_time = local_arrival_time,
                            arrival_seconds_since_midnight = since_midnight,
                            arrival_uncertainty=stu.arrival.uncertainty,
                            schedule_relationship=tu.trip.DESCRIPTOR.enum_types_by_name[
                                'ScheduleRelationship'].values_by_number[tu.trip.schedule_relationship].name
                        )
                        session.add(dbstu)
                        dbtu.StopTimeUpdates.append(dbstu)

                    session.add(dbtu)

            # This does deletes and adds, since it's atomic it never leaves us
            # without data
            session.commit()
        except:
            # else:
            logging.error('Exception occurred in iteration')
            logging.error(sys.exc_info())

        # put this outside the try...except so it won't be skipped when something
        # fails
        # also, makes it easier to end the process with ctrl-c, b/c a
        # KeyboardInterrupt here will end the program (cleanly)
        time.sleep(opts.timeout)

finally:
    logging.info("Closing session . . .")
    session.close()
