from collections import defaultdict
from pathlib import Path
from typing import Union
from census import CensusData
from osm import OpenStreetMapsData
from shapely.geometry import Point
import numpy as np
import geopandas as gpd
import json
from gtfs_functions import Feed
from tqdm import tqdm
import pandas as pd
import random
import networkx as nx

tqdm.pandas()

METERS_TO_DEGREE = 111111 #https://gis.stackexchange.com/questions/2951/algorithm-for-offsetting-a-latitude-longitude-by-some-amount-of-meters
NEARBY_STOP_THRESHOLD = 200
NEARBY_POI_THRESHOLD = 400
#osm = OpenStreetMapsData(37.71496628274509,  -122.54870245854397, 37.80752063920071, -122.35781501086306)

def export_json(d, filename):
    with open(filename, "w+") as f:
        json.dump(d, f)


def find_all_within(origin: Point, options: gpd.GeoSeries, distance_in_meters):
    distances = options.apply(lambda d: approx_distance_in_meters(origin, d))
    mask = distances <= distance_in_meters
    return mask

def find_closest(origin: Point, options: gpd.GeoSeries):
    distances = options.apply(lambda d: approx_distance_in_meters(origin, d))
    idx = np.argmin(distances)
    return idx, distances[idx]

def approx_distance_in_meters(origin: Point, destination: Point):
    x_dist = np.cos(origin.x) * METERS_TO_DEGREE * np.abs(origin.x  - destination.x)
    y_dist =  METERS_TO_DEGREE * np.abs(origin.y  - destination.y)
    return np.sqrt(x_dist ** 2 + y_dist ** 2)


class Dataset:
    def __init__(self, name, gtfs_zip_filename, nearby_stop_threshold = 200, nearby_poi_threshold = 400, census_tables_and_groupings = ("features/census_tables.yaml", "features/census_groupings.yaml"), num_trip_samples=5, save_folder = None, include_delay=False):
        if num_trip_samples % 2 == 0:
            assert Exception("num_trip_sampels must be odd number")
        
        if save_folder is None:
            save_folder = name

        self.gtfs_source = gtfs_zip_filename
        self.feed = Feed(gtfs_zip_filename)
        self.stops_data = gpd.GeoDataFrame(self.feed.stops.copy(), geometry="geometry")
        self.osm = OpenStreetMapsData(self.stops_data.stop_lat.min(), self.stops_data.stop_lon.min(), self.stops_data.stop_lat.max(), self.stops_data.stop_lon.max())
        census_tables, census_groupings = census_tables_and_groupings
        self.census = CensusData(census_tables, census_groupings, geo_cache=f"{name}_census_geo.cache", data_cache=f"{name}_census_data.cache" )
        self.min_distance_fields = []
        self.collapsed_stop_mapping = {}
        self.nearby_stop_threshold = nearby_stop_threshold
        self.nearby_poi_threshold = nearby_poi_threshold

        
        self.save_folder = Path(save_folder)
        save_folder.mkdir(exist_ok=True, parents=True)

        self.num_trip_samples = num_trip_samples 
        self.include_delay = include_delay
        self.G = nx.Graph(directed=False) if not include_delay else nx.DiGraph()

        self.poi_names = []

    @property
    def info(self):
        return {
            "num_trip_samples": self.num_trip_samples,
            "poi_names": self.poi_names,
            "collapsed_stop_mapping": self.collapsed_stop_mapping,
            "nearby_poi_threshold": self.nearby_poi_threshold,
            "nearby_stop_threshold": self.nearby_stop_threshold,
            "census_tables_file": self.census.tables_file,
            "census_groupings_file": self.census.groupings_file,
        }

    def _download_osm_data(self):
        self.min_distance_fields = []

        pois = [
            ("hospital", self.osm.find_hospitals())
            ("grocery", self.osm.find_grocery_store())
            ("park", self.osm.find_parks())
            ("bar", self.osm.find_bars())
            ("worship", self.osm.find_worship())
            ("mcdonalds", self.osm.find_mcdonalds())
            ("starbucks", self.osm.find_starbucks())
        ]

        self.poi_names = [name for (name, _) in pois]

        for poi_name, poi_gdf in tqdm(pois):
            tqdm.write(poi_name)
            self.stops_data[f"closest_{poi_name}_distance"] = self.stops_data.geometry.apply(lambda p: find_closest(p, poi_gdf.geometry)[1])

    def _collapse_stops(self):
        self.collapsed_stop_mapping = {}
        self.stops_data["nearby_stops"] = self.stops_data.progress_apply(lambda row: self.stops_data[find_all_within(row["geometry"], self.stops_data.geometry, self.nearby_stop_threshold)].stop_id.tolist(), axis=1)
        
        i = 0
        with tqdm(total=len( self.stops_data)) as pbar:
            while i < len( self.stops_data):

                mask = self.stops_data.stop_id.apply(lambda s: int(s) not in self.collapsed_stop_mapping.values() and int(s) in map(int, self.stops_data.nearby_stops.iloc[i]))
                data = self.stops_data[mask]
                self.stops_data = self.stops_data[~mask]
                for poi_name in self.poi_names:
                    self.stops_data[f"closest_{poi_name}_distance"].iloc[i] = np.min([self.stops_data[f"closest_{poi_name}_distance"].iloc[i]] + data[f"closest_{poi_name}_distance"].tolist())
                for nb in self.stops_data.nearby_stops.iloc[i]:
                    self.collapsed_stop_mapping[int(nb)] = int(self.stops_data.stop_id.iloc[i])
                
                i += 1
                pbar.n = i
                pbar.total = len(self.stops_data)
                pbar.refresh()
        
        self.stops_data = self.stops_data.reset_index()
        self.collapsed_stop_mapping = {k: self.stops_data.index[ self.stops_data.stop_id == v].values[0] for (k,v) in self.collapsed_stop_mapping.items()}
        self.collapsed_stop_mapping.update({v:self.stops_data.index[ self.stops_data.stop_id == v].values[0] for (_,v) in self.collapsed_stop_mapping.items()})

    def _apply_poi_thresholds(self):
        for poi_name in tqdm(self.poi_names):
            self.stops_data[f"near_{poi_name}"] = self.stops_data[f"closest_{poi_name}_distance"] <= self.nearby_poi_threshold
    
    def _add_census_data(self):
        self.census.add_locations_from_geodataframe(self.stops_data)
        census_data = self.census.get_all_location_data(download=True)
        self.stops_data =  self.stops_data.merge(census_data, on="geometry")

        

    def _link_routes(self):
        self.G = nx.Graph(directed=False) if not self.include_delay else nx.DiGraph()
        trips = self.feed.trips
        stop_times = self.feed.stop_times
   
        sampled_trips = []
        for route_id, group in trips.groupby('route_id'):
            if len(group) >= self.num_trip_samples:
                sampled_trips.extend(random.sample(group['trip_id'].tolist(), self.num_trip_samples))
            else:
                sampled_trips.extend(group['trip_id'].tolist())
        
        stop_times.index = stop_times.trip_id
        stop_times = stop_times[np.array(sampled_trips)]
        stop_times = stop_times.merge(trips[["trip_id", "route_id"]], right_on=trips.trip_id, how="inner")
    
        edge_info = {}

        for stop_idx in self.collapsed_stop_mapping.values():
            self.G.add_node(stop_idx)

        for trip_id in tqdm(sampled_trips):
            trip_stop_times = stop_times[stop_times['trip_id'] == trip_id]

            # Iterate through the stop times for the current trip
            for _, row in tqdm(trip_stop_times.iterrows(),  total=len(trip_stop_times), leave=False):
                stop_id = row['stop_id']
                route_id = row['route_id']

                if len(trip_stop_times) > 1:
                    next_stop = trip_stop_times[trip_stop_times['stop_sequence'] == row['stop_sequence'] + 1]
                    if not next_stop.empty:
                        next_stop_id = next_stop['stop_id'].values[0]

                        stop_idx = self.collapsed_stop_mapping[stop_id]
                        next_stop_idx = self.collapsed_stop_mapping[next_stop_id]

                        cur_driving_time = (trip_stop_times.arrival_time[trip_stop_times.stop_sequence == row['stop_sequence'] + 1].iloc[0] - trip_stop_times.departure_time[trip_stop_times.stop_sequence == row['stop_sequence']].iloc[0]) / 60

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
                            edge_info[(stop_idx, next_stop_idx)]["routes"]

        self.node_attributes = self.stops_data
        self.edge_attriutes = pd.DataFrame(edge_info.values(), index=pd.MultiIndex.from_tuples(edge_info.keys()))
    

    def save(self, folder: Union[str, Path] = None):
        folder = Path(folder) if folder else self.save_folder
        export_json(self.info, folder / "dataset_info.json")
        export_json(nx.node_link_data(self.G), folder / "graph.json")
        self.node_attributes.to_csv(folder / "node_attribtes.csv")
        self.edge_attriutes.to_csv(folder / "edge_attributes.csv")


    def json_cache(self, **kwargs):
        for name, obj in kwargs.items():
            export_json(obj, self.save_folder / name + ".json.cache")

    
    def csv_cache(self, **kwargs):
        for name, df in kwargs.items():
            df.to_csv(self.save_folder / name + ".csv.cache")

    def build(self, save_folder: Union[str, Path] = None):
        self._download_osm_data()
        self.json_cache("dataset_info", self.info)
        self.csv_cache("osm_stops_data", self.stops_data)
        
        self._collapse_stops()
        self._apply_poi_thresholds()
        self.json_cache("collapsed_stop_mapping", self.collapsed_stop_mapping)
        self.csv_cache("collapse_stops_data", self.stops_data)

        self._add_census_data()
        self.csv_cache("census_stops_data", self.stops_data)

        self._link_routes()
        self.save(save_folder)



if __name__ == '__main__':
    main()




