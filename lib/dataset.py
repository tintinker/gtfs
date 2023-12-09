import ast
from collections import defaultdict
from sqlalchemy import create_engine
from pathlib import Path
from typing import Union
from lib.census import CensusData
from lib.queries import delay_query
from lib.osm import OpenStreetMapsData
from shapely import from_wkt
import numpy as np
import geopandas as gpd
import json
from tqdm import tqdm
import pandas as pd
import random
import networkx as nx
import lib.util as util
import shutil
import logging
logging.basicConfig(level=logging.DEBUG)

tqdm.pandas()

SECONDS_TO_MINUTES = 60


class Dataset:
    def __init__(self, name, gtfs_zip_filename, nearby_stop_threshold = 200, nearby_poi_threshold = 400, census_tables_and_groupings = ("lib/census_tables.yaml", "lib/census_groupings.yaml"), num_trip_samples=5, save_folder = None, include_delay=False, delay_sqlite_db_str = None, delay_max = 30, already_built=False, include_census=True, census_boundaries_file=None):
        if num_trip_samples % 2 == 0:
            assert Exception("num_trip_sampels must be odd number")
        
        if include_delay and not delay_sqlite_db_str:
            assert Exception("SQLITE connection string required for delay info")

        if save_folder is None:
            save_folder = name
        
        self.save_folder = Path(save_folder)
        self.save_folder.mkdir(exist_ok=True, parents=True)
       
        self.logger = logging.getLogger(f"dataset_builder [{save_folder}]")
        handler = logging.FileHandler(self.save_folder / "dataset.log")
        self.logger.addHandler(handler)

        self.name = name
        self.gtfs_source = gtfs_zip_filename
        self.stops, self.trips, self.stop_times, self.routes = util.load_gtfs_zip(gtfs_zip_filename)
        self.stops_data = self.stops.copy().set_index('stop_id', drop=False)

        self.osm = OpenStreetMapsData(self.stops_data.stop_lat.min(), self.stops_data.stop_lon.min(), self.stops_data.stop_lat.max(), self.stops_data.stop_lon.max(), logger=self.logger)
        self.cosine_latitude = np.cos(self.stops_data.stop_lat.median())
        
        self.include_census = include_census
        census_tables, census_groupings = census_tables_and_groupings
        self.census = CensusData(census_tables, census_groupings, census_boundaries_file=census_boundaries_file, geo_cache = self.save_folder / f"{name}_census_geo.cache", data_cache= self.save_folder / f"{name}_census_data.cache", logger=self.logger)
        
        self.nearby_stop_threshold = nearby_stop_threshold
        self.nearby_poi_threshold = nearby_poi_threshold

        self.num_trip_samples = num_trip_samples 
        self.include_delay = include_delay
        self.delay_sqlite_db_str = delay_sqlite_db_str if delay_sqlite_db_str else ""
        self.delay_max = delay_max

        self.G = nx.DiGraph()

        self.poi_names = []

        self.built = already_built

    @property
    def info(self):
        return {
            "name": self.name,
            "save_folder": str(self.save_folder),
            "gtfs_source": self.gtfs_source,
            "num_trip_samples": self.num_trip_samples,
            "poi_names": self.poi_names,
            "nearby_poi_threshold": self.nearby_poi_threshold,
            "nearby_stop_threshold": self.nearby_stop_threshold,
            "census_tables_file": self.census.tables_file,
            "census_groupings_file": self.census.groupings_file,
            "delay_sqlite_db_str": self.delay_sqlite_db_str,
            "delay_max": self.delay_max,
            "built": self.built,
            "include_delay": self.include_delay
        }

    @property
    def all_node_attribute_names(self):
        return list(self.node_attributes.columns)
    
    @property
    def all_edge_attribute_names(self):
        return list(self.edge_attriutes.columns)
    
    @staticmethod
    def load(folder: Union[str, Path]):
        folder = Path(folder)

        with open(folder / "dataset_info.json") as f:
            dataset_info = json.load(f)
        
        with open(folder / "graph.json") as f:
            graph = json.load(f)
        
        dataset = Dataset(dataset_info["name"], dataset_info["gtfs_source"], dataset_info["nearby_stop_threshold"], dataset_info["nearby_poi_threshold"], (dataset_info["census_tables_file"], dataset_info["census_groupings_file"]), dataset_info["num_trip_samples"], dataset_info["save_folder"], dataset_info["include_delay"], dataset_info["delay_sqlite_db_str"], dataset_info["delay_max"], dataset_info["built"])
        dataset.G = nx.node_link_graph(graph)

        dataset.node_attributes = pd.read_csv(folder / "node_attribtes.csv", index_col=0, dtype=util.DATA_TYPES).drop_duplicates()
        try:
            dataset.node_attributes =  dataset.node_attributes.set_index('stop_id', drop=False)
        except:
            dataset.node_attributes["stop_id"] = dataset.node_attributes.index
            
        dataset.node_attributes.geometry = dataset.node_attributes.geometry.apply(from_wkt)
        dataset.node_attributes = gpd.GeoDataFrame(dataset.node_attributes, geometry="geometry")

        def load_route_list(s):
            try:
                return ast.literal_eval(s)
            except:
                return []
            
        dataset.node_attributes.routes = dataset.node_attributes.routes.apply(load_route_list)
        dataset.edge_attriutes = pd.read_csv(folder / "edge_attributes.csv", index_col=[0,1], dtype=util.DATA_TYPES).drop_duplicates()
        return dataset
   
    
    def save(self, folder: Union[str, Path] = None):
        folder = Path(folder) if folder else self.save_folder

        try:
            shutil.copy2(self.gtfs_source, folder / Path(self.gtfs_source).name)
            self.gtfs_source = str(folder / Path(self.gtfs_source).name)
        except Exception as e:
            self._debug(f"Could not copy over gtfs zip, skipping: {e}")

        util.export_json(self.info, folder / "dataset_info.json")
        util.export_json(nx.node_link_data(self.G), folder / "graph.json")
        self.node_attributes.drop_duplicates().to_csv(folder / "node_attribtes.csv")
        self.edge_attriutes.drop_duplicates().to_csv(folder / "edge_attributes.csv")
        
        for file_path in folder.glob("*"):
            if file_path.is_file() and file_path.suffix == ".cache":
                file_path.unlink() 


    def _build(self, override_if_already_built = False, use_cache = True, save_folder: Union[str, Path] = None):
        if self.built and not override_if_already_built:
            raise Exception("Dataset already built and override set to false. If you'd like to force a rebuild, set override to true")
        
        with tqdm(total=4, desc="build progress") as pbar:
            self._debug("\nStep 1: Downloading OSM Data")
            
            if use_cache:
                _, info_ok = self._from_json_cache("dataset_info")
                stops_data, osm_ok =  self._from_csv_cache("osm_stops_data")
            if use_cache and info_ok and osm_ok:
                self.stops_data = stops_data
                self._debug("Loaded OSM from cache")
            else:
                self._download_osm_data()
                self._apply_poi_thresholds()
                self._to_json_cache("dataset_info", self.info)
                self._to_csv_cache("osm_stops_data", self.stops_data)
            pbar.update(1)

            if self.include_census:
                self._debug("\nStep 2: Adding Census Data")

                if use_cache:
                    stops_data, census_ok = self._from_csv_cache("all_stops_data")
                
                if self.include_census and use_cache and census_ok:
                    self.stops_data = stops_data
                    self._debug("Loaded Census info cache")
                elif self.include_census:
                    self._add_census_data()
                    self._to_csv_cache("all_stops_data", self.stops_data)
            pbar.update(1)


            if self.include_delay:
                self._debug("\nStep 3: Adding Delay Data")
                
                if use_cache:
                    delay_df, delay_ok =  self._from_csv_cache("delay_df")
                
                if use_cache and delay_ok:
                    self.delay_df = delay_df
                    self._debug("Loaded Delay info cache")
                else:
                    self._download_delay_info()
                    self._to_csv_cache("delay_df", self.delay_df)
            pbar.update(1)


            self._debug("\nStep 4: Linking Routes")
            self._link_routes()
            pbar.update(1)
            
            self.save(save_folder)
            self._debug("Saved to " + str(self.save_folder))


    def _download_osm_data(self):
        pois = [
            ("hospital", self.osm.find_hospitals()),
            ("grocery", self.osm.find_grocery_store()),
            ("park", self.osm.find_parks()),
            ("bar", self.osm.find_bars()),
            ("worship", self.osm.find_worship()),
            ("mcdonalds", self.osm.find_mcdonalds()),
            ("starbucks", self.osm.find_starbucks())
        ]

        self.poi_names = [name for (name, _) in pois]

        for poi_name, poi_gdf in tqdm(pois, desc="join osm data"):
            self._debug("joining data for: " + poi_name)
            self.stops_data[f"closest_{poi_name}_distance"] = self.stops_data.geometry.apply(lambda p: util.find_closest(p, poi_gdf.geometry, self.cosine_latitude)[1])

    def _apply_poi_thresholds(self):
        for poi_name in tqdm(self.poi_names, desc="apply poi thresholds"):
            self.stops_data[f"near_{poi_name}"] = self.stops_data[f"closest_{poi_name}_distance"] <= self.nearby_poi_threshold
    
    def _add_census_data(self):
        self.census.add_locations_from_geodataframe(self.stops_data)
        census_data = self.census.get_all_location_data(download=True)
        self.stops_data =  self.stops_data.merge(census_data, on="geometry", how="left")

    def _download_delay_info(self):
        conn = create_engine(f"sqlite:///{self.delay_sqlite_db_str}")

        self.delay_df = pd.read_sql_query(delay_query(), conn)

        self.delay_df = self.delay_df.groupby(['stop_id', 'trip_sequence']).agg({
            'trip_id': 'first',
            'route_id': 'first',
            'stop_name': 'first',
            'route_name': 'first',
            'minute_delay': 'mean',
            'oid': 'max',
            'actual_arrival_time': ['max', 'min'],
            'planned_arrival_time': ['max', 'min']}).reset_index()

        self.delay_df.columns = self.delay_df.columns.to_flat_index().map(lambda x: x[0]+"_"+x[1] if x[1] == 'max' or x[1] == 'min' else x[0])
        self.delay_df.minute_delay = self.delay_df.minute_delay.clip(0, self.delay_max)

    def _link_routes(self):
        self.G = nx.DiGraph()
        trips = self.trips.copy()
        stop_times = self.stop_times.copy()

        stop_times = stop_times.merge(trips[["trip_id","route_id"]], on="trip_id", how="left")
   
        sampled_trips = []
        stop_times.route_id = stop_times.route_id.astype(str)
        for route_id in stop_times.route_id.unique():
            sampled_trips.append(random.choice(stop_times.trip_id[stop_times.route_id == route_id].unique()))
            
        
        stop_times = stop_times[stop_times.trip_id.isin(sampled_trips)]
       
        edge_info = {}
        stops_to_routes = defaultdict(set)

        for stop_idx in self.stops_data.index:
            self.G.add_node(stop_idx)

        for trip_id in tqdm(sampled_trips, desc="linking trips"):
            trip_stop_times = stop_times[stop_times.trip_id == trip_id].sort_values('stop_sequence').reset_index()

            if len(trip_stop_times) < 2:
                continue

            assert len(trip_stop_times.route_id.unique()) == 1
            route_id = trip_stop_times.route_id.iloc[0]

            # Iterate through the stop times for the current trip
            for i in tqdm(range(1, len(trip_stop_times)), leave=False):

                stop_id = trip_stop_times.stop_id.iloc[i]
                prev_stop_id = trip_stop_times.stop_id.iloc[i - 1]

                if prev_stop_id == stop_id:
                    continue

                cur_driving_time = (trip_stop_times.arrival_time.iloc[i] - trip_stop_times.departure_time.iloc[i - 1]) / SECONDS_TO_MINUTES

                edge = (prev_stop_id, stop_id)
                if not self.G.has_edge(*edge):
                    self.G.add_edge(*edge)
                    edge_info[edge] = {
                        "update_count": 1,
                        "driving_time": cur_driving_time,
                        "routes": [route_id]
                    }
                        
                else:
                    update_count = edge_info[edge]["update_count"]
                    prev_driving_time = edge_info[edge]["driving_time"]
                    
                    edge_info[edge]["update_count"] = update_count + 1
                    edge_info[edge]["driving_time"] = (1/update_count) * cur_driving_time + (1 - 1/update_count) * prev_driving_time
                    edge_info[edge]["routes"].append(route_id)
                    
                    stops_to_routes[prev_stop_id].add(route_id)
                    stops_to_routes[stop_id].add(route_id) 

                    if self.include_delay:
                        delay_info = self.delay_df[(self.delay_df.stop_id == stop_id) & (self.delay_df.trip_sequence == trip_stop_times.stop_sequence.iloc[i])]
                        avg_delay = np.nan if delay_info.empty else delay_info.minute_delay.iloc[0]
                        edge_info[(prev_stop_id, stop_idx)]["avg_delay"] = avg_delay

        for edge in edge_info:
            edge_info[edge]["routes"] = tuple(edge_info[edge]["routes"])

        stops_to_routes = {k:tuple(v) for k,v in stops_to_routes.items()}
        self.stops_data["routes"] = pd.Series(stops_to_routes)
        
        self.node_attributes = self.stops_data
        self.edge_attriutes = pd.DataFrame(edge_info.values(), index=pd.MultiIndex.from_tuples(edge_info.keys()))


    def _to_json_cache(self, name, obj):
        util.export_json(obj, self.save_folder / (name + ".json.cache"))

    
    def _to_csv_cache(self, name, df):
        df.to_csv(self.save_folder / (name + ".csv.cache"))

    def _from_json_cache(self, name):
        if not Path(self.save_folder / (name + ".json.cache")).is_file():
            return None, False
         
        with open(self.save_folder / (name + ".json.cache")) as f:
            return json.load(f), True

    def _from_csv_cache(self, name):
        if not Path(self.save_folder / (name + ".csv.cache")).is_file():
            return None, False
        
        df = pd.read_csv(self.save_folder / (name + ".csv.cache"), dtype=util.DATA_TYPES)
        if "geometry" not in df.columns:
            return df, True
        
        df.geometry = df.geometry.apply(from_wkt)
        gdf = gpd.GeoDataFrame(df, geometry="geometry")
        return gdf, True

    def _debug(self, *messages):
        message = " ".join([str(m) for m in messages])
        self.logger.debug(message)
        tqdm.write(message)