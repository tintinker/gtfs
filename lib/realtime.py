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

WAIT_PERIOD = 15

Base = declarative_base()

class TripUpdate(Base):
    __tablename__ = 'trip_updates'
    oid = Column(Integer, primary_key=True)

    trip_id = Column(String(64))
    route_id = Column(String(64))
    trip_start_time = Column(String(8))
    trip_start_date = Column(String(10))

    schedule_relationship = Column(String(9))

    vehicle_id = Column(String(64))
    vehicle_label = Column(String(255))
    vehicle_license_plate = Column(String(10))

    timestamp = Column(DateTime)

    StopTimeUpdates = relationship('StopTimeUpdate', backref='TripUpdate')


class StopTimeUpdate(Base):
    __tablename__ = 'stop_time_updates'
    oid = Column(Integer, primary_key=True)

    stop_sequence = Column(Integer)
    stop_id = Column(String(10))

    arrival_time = Column(DateTime)
    arrival_seconds_since_midnight = Column(Integer)
    arrival_uncertainty = Column(Integer)

    schedule_relationship = Column(String(9))

    trip_update_id = Column(Integer, ForeignKey('trip_updates.oid'))

AllClasses = (TripUpdate, StopTimeUpdate)


#'UTC Time Zone Offset (ex. -8 for Pacific Standard Time)'
class RealtimeWatcher:
    def __init__(self, gtfs_filename, realtime_updates_url, timezone, save_folder, api_key = None, resuming_from_previous=False):
        self.realtime_updates_url = realtime_updates_url
        self.timezone = timezone
        self.api_key = api_key

        save_folder = Path(save_folder)
        save_folder.mkdir(exist_ok=True, parents=True)


        self.logger = logging.getLogger(f"realtime_watcher [{save_folder}]")
        handler = logging.FileHandler(save_folder / "realtime.log")
        self.logger.addHandler(handler)


        sqlitedb_file = save_folder / "realtime.db"
        self.engine = create_engine(f"sqlite:///{str(sqlitedb_file)}")

        if not resuming_from_previous:
            stops, trips, stop_times, routes = util.load_gtfs_zip(gtfs_filename)

            routes.to_sql(name='routes', con=self.engine)
            trips.to_sql(name='trips', con=self.engine)
            stops.drop(columns='geometry').to_sql(name='stops', con=self.engine)
            stop_times.to_sql(name='stop_times', con=self.engine)

            self.logger.debug("Saved static data")

        insp = inspect(self.engine)
        for table in Base.metadata.tables.keys():
            if not insp.has_table(table):
                self.logger.info('Creating table %s', table)
                Base.metadata.tables[table].create(self.engine)


    def watch(self):
        t = Thread(target=self._watch) 
        t.start()
        return t
    
    def _watch(self):
        self.logger.debug(f"Starting Realtime Tracking: {datetime.now()}")
        self.session = sessionmaker(bind=self.engine)()
        try:
            while True:
                try:
                    fm = gtfs_realtime_pb2.FeedMessage()
                    fm.ParseFromString(
                        urlopen(util.format_req(self.realtime_updates_url, self.api_key)).read()
                    )

                    timestamp = datetime.utcfromtimestamp(fm.header.timestamp)

                    if fm.header.gtfs_realtime_version != u'1.0':
                        self.logger.debug('Warning: feed version has changed: found %s, expected 1.0', fm.header.gtfs_realtime_version)

                    self.logger.info('Adding %s trip updates', len(fm.entity))
                    for entity in fm.entity:

                        tu = entity.trip_update

                        dbtu = TripUpdate(
                            trip_id=tu.trip.trip_id,
                            route_id=tu.trip.route_id,
                            trip_start_time=tu.trip.start_time,
                            trip_start_date=tu.trip.start_date,

                            schedule_relationship=tu.trip.DESCRIPTOR.enum_types_by_name[
                                'ScheduleRelationship'].values_by_number[tu.trip.schedule_relationship].name,

                            vehicle_id=tu.vehicle.id,
                            vehicle_label=tu.vehicle.label,
                            vehicle_license_plate=tu.vehicle.license_plate,
                            timestamp=timestamp)

                        for stu in tu.stop_time_update:
                            local_arrival_time = datetime.utcfromtimestamp(stu.arrival.time) + timedelta(hours = self.timezone)
                            since_midnight = util.seconds_since_midnight(local_arrival_time)
                            dbstu = StopTimeUpdate(
                                stop_sequence=stu.stop_sequence,
                                stop_id=stu.stop_id,
                                arrival_time = local_arrival_time,
                                arrival_seconds_since_midnight = since_midnight,
                                arrival_uncertainty=stu.arrival.uncertainty,
                                schedule_relationship=tu.trip.DESCRIPTOR.enum_types_by_name[
                                    'ScheduleRelationship'].values_by_number[tu.trip.schedule_relationship].name
                            )
                            self.session.add(dbstu)
                            dbtu.StopTimeUpdates.append(dbstu)

                        self.session.add(dbtu)

                    # This does deletes and adds, since it's atomic it never leaves us
                    # without data
                    self.session.commit()
                except:
                    self.logger.error('Exception occurred in iteration')
                    self.logger.error(sys.exc_info())

                time.sleep(WAIT_PERIOD)
        finally:
            self.logger.info("Closing session . . .")
            self.logger.debug(f"Ending Realtime Tracking: {datetime.now()}")
            self.session.close()

if __name__ == "__main__":
    realtime_watcher = RealtimeWatcher(gtfs_filename, realtime_url, timezone, save_folder, api_key, resume_from_previous)
    realtime_watcher.watch()
