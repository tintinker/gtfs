from datetime import datetime, timedelta
from threading import Thread
import time
import sys
from urllib.request import urlopen
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from google.transit import gtfs_realtime_pb2
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, ForeignKey, Integer, String, DateTime
from sqlalchemy.orm import relationship
import logging
from pathlib import Path
from sqlalchemy import create_engine
import lib.util as util
logging.basicConfig(level=logging.DEBUG)

Base = declarative_base()


sqlitedb_file = "realtime/la/realtime.db"
engine = create_engine(f"sqlite:///{str(sqlitedb_file)}")
print(sqlitedb_file, engine)
stops, trips, stop_times, routes = util.load_gtfs_zip("gtfs_data/2023_december/los_angeles_gtfs.zip")
print("loaded")
routes.to_sql(name='routes', con=engine)
trips.to_sql(name='trips', con=engine)
stops.drop(columns='geometry').to_sql(name='stops', con=engine)
stop_times.to_sql(name='stop_times', con=engine)

print("Saved static data")

insp = inspect(engine)
for table in Base.metadata.tables.keys():
    if not insp.has_table(table):
        print('Creating table', table)
        Base.metadata.tables[table].create(engine)