import ast
from collections import defaultdict
from sqlalchemy import create_engine
from pathlib import Path
from typing import Union
from gtfs.lib.census import CensusData
from queries import delay_query
from gtfs.lib.osm import OpenStreetMapsData
from shapely import from_wkt
import numpy as np
import geopandas as gpd
import json
from gtfs_functions import Feed
from tqdm import tqdm
import pandas as pd
import random
import networkx as nx
import util
import shutil

tqdm.pandas()

SECONDS_TO_MINUTES = 60


class Dataset:
    def __init__(self, name, gtfs_zip_filename, nearby_stop_threshold = 200, nearby_poi_threshold = 400, census_tables_and_groupings = ("features/census_tables.yaml", "features/census_groupings.yaml"), num_trip_samples=5, save_folder = None, include_delay=False, delay_sqlite_db_str = None, delay_max = 30, already_built=False):
        if num_trip_samples % 2 == 0:
            assert Exception("num_trip_sampels must be odd number")
        
        if include_delay and not delay_sqlite_db_str:
            assert Exception("SQLITE connection string required for delay info")

        if save_folder is None:
            save_folder = name
        
        self.save_folder = Path(save_folder)
        self.save_folder.mkdir(exist_ok=True, parents=True)

        self.name = name
        self.gtfs_source = gtfs_zip_filename
        self.feed = Feed(gtfs_zip_filename, patterns=False)
        self.stops_data = gpd.GeoDataFrame(self.feed.stops.copy(), geometry="geometry")
        self.stops_data.stop_id = self.stops_data.stop_id.astype(str)

        self.osm = OpenStreetMapsData(self.stops_data.stop_lat.min(), self.stops_data.stop_lon.min(), self.stops_data.stop_lat.max(), self.stops_data.stop_lon.max())
        self.cosine_of_longitude = np.cos(self.stops_data.stop_lon.median() / (2*np.pi))
        census_tables, census_groupings = census_tables_and_groupings
        self.census = CensusData(census_tables, census_groupings, geo_cache = self.save_folder / f"{name}_census_geo.cache", data_cache= self.save_folder / f"{name}_census_data.cache" )
        self.min_distance_fields = []
        self.collapsed_stop_mapping = {}
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
            "collapsed_stop_mapping": self.collapsed_stop_mapping,
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
        
        with open(folder / "collapsed_stop_mapping.json") as f:
            collapsed_stop_mapping = json.load(f)
        
        with open(folder / "graph.json") as f:
            graph = json.load(f)
        
        dataset = Dataset(dataset_info["name"], dataset_info["gtfs_source"], dataset_info["nearby_stop_threshold"], dataset_info["nearby_poi_threshold"], (dataset_info["census_tables_file"], dataset_info["census_groupings_file"]), dataset_info["num_trip_samples"], dataset_info["save_folder"], dataset_info["include_delay"], dataset_info["delay_sqlite_db_str"], dataset_info["delay_max"], dataset_info["built"])
        dataset.G = nx.node_link_graph(graph)
        dataset.collapsed_stop_mapping = collapsed_stop_mapping

        dataset.node_attributes = pd.read_csv(folder / "node_attribtes.csv", index_col=0)
        dataset.node_attributes.geometry = dataset.node_attributes.geometry.apply(from_wkt)
        dataset.node_attributes = gpd.GeoDataFrame(dataset.node_attributes, geometry="geometry")

        def load_route_list(s):
            try:
                return ast.literal_eval(s)
            except:
                return []
            
        dataset.node_attributes.routes = dataset.node_attributes.routes.apply(load_route_list)

        dataset.edge_attriutes = pd.read_csv(folder / "edge_attributes.csv", index_col=[0,1])
        return dataset
   
    
    def save(self, folder: Union[str, Path] = None):
        folder = Path(folder) if folder else self.save_folder

        try:
            shutil.copy2(self.gtfs_source, folder / Path(self.gtfs_source).name)
            self.gtfs_source = str(folder / Path(self.gtfs_source).name)
        except Exception as e:
            print(f"Could not copy over gtfs zip, skipping: {e}")

        util.export_json(self.info, folder / "dataset_info.json")
        util.export_json(nx.node_link_data(self.G), folder / "graph.json")
        util.export_json(self.collapsed_stop_mapping, folder / "collapsed_stop_mapping.json")
        self.node_attributes.to_csv(folder / "node_attribtes.csv")
        self.edge_attriutes.to_csv(folder / "edge_attributes.csv")
        
        for file_path in folder.glob("*"):
            if file_path.is_file() and file_path.suffix == ".cache":
                file_path.unlink() 

    def _build(self, override = False, save_folder: Union[str, Path] = None):
        if self.built and not override:
            raise Exception("Dataset already built and override set to false. If you'd like to force a rebuild, set override to true")
        
        with tqdm(total=5, desc="build progress") as pbar:
            tqdm.write("\nStep 1: Downloading OSM Data")
            self._download_osm_data()
            self._to_json_cache("dataset_info", self.info)
            self._to_csv_cache("osm_stops_data", self.stops_data)
            pbar.update(1)
            
            tqdm.write("\nStep 2: Collapsing Stops and Applying POI Thresholds")
            self._collapse_stops()
            self._apply_poi_thresholds()
            self._to_json_cache("collapsed_stop_mapping", self.collapsed_stop_mapping)
            self._to_csv_cache("collapse_stops_data", self.stops_data)
            pbar.update(1)


            tqdm.write("\nStep 3: Adding Census Data")
            self._add_census_data()
            self._to_csv_cache("all_stops_data", self.stops_data)
            pbar.update(1)

            self.collapsed_stop_mapping = self._from_json_cache("collapsed_stop_mapping")
            self.stops_data = self._from_csv_cache("all_stops_data")


            if self.include_delay:
                tqdm.write("\nStep 4: Adding Delay Data")
                self._download_delay_info()
                self._to_csv_cache("delay_df", self.delay_df)
            pbar.update(1)


            tqdm.write("\nFinal Step: Linking Routes")
            self._link_routes()
            pbar.update(1)
            
            self.save(save_folder)
            tqdm.write("Saved to " + str(self.save_folder))


    def _download_osm_data(self):
        self.min_distance_fields = []

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
            tqdm.write("joining data for: " + poi_name)
            self.stops_data[f"closest_{poi_name}_distance"] = self.stops_data.geometry.apply(lambda p: util.find_closest(p, poi_gdf.geometry)[1])

    def _collapse_stops(self):
        self.collapsed_stop_mapping = {}
        self.stops_data["nearby_stops"] = self.stops_data.progress_apply(lambda row: self.stops_data[util.find_all_within(row["geometry"], self.stops_data.geometry, self.nearby_stop_threshold)].stop_id.tolist(), axis=1)
       
        i = 0
        with tqdm(total=len(self.stops_data), desc="removing duplicate stops") as pbar:
            while i < len( self.stops_data):

                mask = self.stops_data.stop_id.apply(lambda s: str(s) != str(self.stops_data.stop_id.iloc[i]) and str(s) not in self.collapsed_stop_mapping.values() and str(s) in map(str, self.stops_data.nearby_stops.iloc[i]))
                data = self.stops_data[mask]
                for poi_name in self.poi_names:
                    self.stops_data[f"closest_{poi_name}_distance"].iloc[i] = np.min([self.stops_data[f"closest_{poi_name}_distance"].iloc[i]] + data[f"closest_{poi_name}_distance"].tolist())
                for nb in self.stops_data.nearby_stops.iloc[i]:
                    self.collapsed_stop_mapping[str(nb)] = str(self.stops_data.stop_id.iloc[i])

                self.stops_data = self.stops_data[~mask]
            
                i += 1
                pbar.n = i
                pbar.total = len(self.stops_data)
                pbar.refresh()
        
        self.stops_data = self.stops_data.reset_index()
        
        isolated_stops_map = {str(si):int(self.stops_data.index[self.stops_data.stop_id == si].values[0]) for si in self.stops_data.stop_id if si not in self.collapsed_stop_mapping}
        value_to_value_map = {str(v):int(self.stops_data.index[self.stops_data.stop_id == v].values[0]) for (_,v) in self.collapsed_stop_mapping.items()}
        self.collapsed_stop_mapping = {str(k): int(self.stops_data.index[self.stops_data.stop_id == v].values[0]) for (k,v) in self.collapsed_stop_mapping.items()}
        self.collapsed_stop_mapping.update(value_to_value_map)
        self.collapsed_stop_mapping.update(isolated_stops_map)

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
        trips = self.feed.trips
        stop_times = self.feed.stop_times
   
        sampled_trips = []
        for route_id, group in trips.groupby('route_id'):
            if len(group) >= self.num_trip_samples:
                sampled_trips.extend(random.sample(group['trip_id'].tolist(), self.num_trip_samples))
            else:
                sampled_trips.extend(group['trip_id'].tolist())
        
        stop_times = stop_times.merge(trips[["trip_id","route_id"]], how="left")
        
        stop_times.index = stop_times.trip_id
        stop_times = stop_times.loc[np.array(sampled_trips)]
        edge_info = {}
        stops_to_routes = defaultdict(list)

        for stop_idx in self.collapsed_stop_mapping.values():
            self.G.add_node(stop_idx)

        for trip_id in tqdm(sampled_trips, desc="linking trips"):
            trip_stop_times = stop_times[stop_times.trip_id == trip_id]

            if len(trip_stop_times) < 2:
                continue

            # Iterate through the stop times for the current trip
            for _, row in tqdm(trip_stop_times.iterrows(),  total=len(trip_stop_times), leave=False):
                stop_id = row['stop_id']
                route_id = row['route_id']

               
                next_stop = trip_stop_times[trip_stop_times['stop_sequence'] == row['stop_sequence'] + 1]
                if not next_stop.empty:
                    next_stop_id = next_stop['stop_id'].values[0]

                    stop_idx = self.collapsed_stop_mapping[str(stop_id)]
                    next_stop_idx = self.collapsed_stop_mapping[str(next_stop_id)]

                    if stop_idx == next_stop_idx:
                        continue

                    cur_driving_time = (trip_stop_times.arrival_time[trip_stop_times.stop_sequence == row['stop_sequence'] + 1].iloc[0] - trip_stop_times.departure_time[trip_stop_times.stop_sequence == row['stop_sequence']].iloc[0]) / SECONDS_TO_MINUTES

                    if not self.G.has_edge(stop_idx, next_stop_idx):
                        self.G.add_edge(stop_idx, next_stop_idx)
                        edge_info[(stop_idx, next_stop_idx)] = {
                            "update_count": 1,
                            "driving_time": cur_driving_time,
                            "routes": [route_id]
                        }
                        
                    else:
                        update_count = edge_info[(stop_idx, next_stop_idx)]["update_count"]
                        prev_driving_time = edge_info[(stop_idx, next_stop_idx)]["driving_time"]
                        
                        edge_info[(stop_idx, next_stop_idx)]["update_count"] = update_count + 1
                        edge_info[(stop_idx, next_stop_idx)]["driving_time"] = (1/update_count) * cur_driving_time + (1 - 1/update_count) * prev_driving_time
                        edge_info[(stop_idx, next_stop_idx)]["routes"].append(route_id)
                        stops_to_routes[stop_idx].append(route_id)
                        stops_to_routes[next_stop_idx].append(route_id) 

                    if self.include_delay:
                        delay_info = self.delay_df[(self.delay_df.stop_id == next_stop_id) & (self.delay_df.trip_sequence == row['stop_sequence'] + 1)]
                        avg_delay = np.nan if delay_info.empty else delay_info.minute_delay.iloc[0]
                        edge_info[(stop_idx, next_stop_idx)]["avg_delay"] = avg_delay

        self.stops_data["routes"] = pd.Series(stops_to_routes)
        
        self.node_attributes = self.stops_data
        self.edge_attriutes = pd.DataFrame(edge_info.values(), index=pd.MultiIndex.from_tuples(edge_info.keys()))


    def _to_json_cache(self, name, obj):
        util.export_json(obj, self.save_folder / (name + ".json.cache"))

    
    def _to_csv_cache(self, name, df):
        df.to_csv(self.save_folder / (name + ".csv.cache"))

    def _from_json_cache(self, name):
        with open(self.save_folder / (name + ".json.cache")) as f:
            return json.load(f)

    def _from_csv_cache(self, name):
        df = pd.read_csv(self.save_folder / (name + ".csv.cache"))
        if "geometry" not in df.columns:
            return df
        df.geometry = df.geometry.apply(from_wkt)
        gdf = gpd.GeoDataFrame(df, geometry="geometry")
        return gdf

   