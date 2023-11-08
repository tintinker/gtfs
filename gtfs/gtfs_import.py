from gtfs_functions import Feed
from sqlalchemy import create_engine
from optparse import OptionParser
import pandas as pd
from datetime import datetime, timedelta

EPOCH = datetime(1970, 1, 1, 0, 0, 0)

def to_stamp(st):
    return EPOCH + timedelta(seconds = int(st))

def main():
    p = OptionParser()

    p.add_option('-f', '--file', dest='gtfsFile', default=None,
                help='The zip file containing the GTFS Data', metavar='FILE.zip')

    p.add_option('-d', '--database', default=None, dest='dsn',
                help='Database connection string', metavar='DSN')
    

    opts, args = p.parse_args()

    if not opts.gtfsFile or not opts.dsn:
        raise Exception("GTFS File and Datanase connection string required")
    

    engine = create_engine(opts.dsn)
    feed = Feed(opts.gtfsFile, patterns=False)

    routes = feed.routes
    trips = feed.trips
    stops = pd.DataFrame(feed.stops.drop(columns='geometry'))
    stop_times = pd.DataFrame(feed.stop_times.drop(columns='geometry'))
    shapes = pd.DataFrame(feed.shapes.drop(columns='geometry'))
    
    stop_times["arrival_seconds_since_midnight"] = stop_times.arrival_time
    stop_times["arrival_timestamp"] = pd.to_datetime(stop_times.arrival_time.apply(to_stamp))


    routes.to_sql(name='routes', con=engine)
    trips.to_sql(name='trips', con=engine)
    stops.to_sql(name='stops', con=engine)
    stop_times.to_sql(name='stop_times', con=engine)
    shapes.to_sql(name='shapes', con=engine)

if __name__ == "__main__":
    main()