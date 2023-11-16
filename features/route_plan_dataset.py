from pathlib import Path
from typing import Union
from shapely import from_wkt

import numpy as np
from dataset import Dataset
import random
import geopandas as gpd
import os
from shapely.geometry import LineString
import util
from torch_geometric.utils import from_networkx
import networkx as nx
import contextily as ctx
import matplotlib.pyplot as plt
from ast import literal_eval
import pandas as pd
from shapely import Point
from tqdm import tqdm

SECONDS_TO_MINUTES = 60
FIVE_HOURS_IN_MINUTES = 5 * 60
ONE_DAY_IN_MINUTES = 24 * 60
WALKING_DISTANCE_METERS = 400
TRANSFER_PENALTY_MINUTES = 5
STOP_PENALTY_MINUTES = 0.5

class RoutePlanDataset(Dataset):
    @staticmethod
    def load(folder):
        dataset = Dataset.load(folder)
        dataset.__class__ = RoutePlanDataset
        return dataset
    
    def save(self, save_folder: Union[str, Path] = None):
        save_folder = Path(save_folder) if save_folder else self.save_folder
        super().save(save_folder)
        self.original_route_info.to_csv(save_folder / "original_route_info.csv")
        self.current_route_info.to_csv(save_folder / "current_route_info.csv")

    
    def build(self, save_folder: Union[str, Path] = None):
        #super().build(save_folder)
        self.get_original_route_info()
        self.current_route_info = pd.DataFrame([], columns=["route_id", "shortest_interval", "first_trip_time", "last_trip_time", "collapsed_stop"])  
        self.save(save_folder)

    def get_original_route_info(self):
        routes = self.feed.routes
        stop_times = self.feed.stop_times[['route_id', 'trip_id', 'stop_id', 'stop_sequence', 'departure_time']]
        first_departure_times = stop_times[stop_times.stop_sequence == 1].sort_values('departure_time')
        routes_frequencies_stops = [] #minutes between trips
        for route_id in tqdm(routes.route_id.unique()):
            frequency_by_origin = []
            route_times = first_departure_times[first_departure_times.route_id == route_id]
            for first_stop_id in route_times.stop_id.unique():
                freq = route_times[route_times.stop_id == first_stop_id].departure_time.diff().median() / SECONDS_TO_MINUTES
                first_trip_id = route_times[route_times.stop_id == first_stop_id].trip_id.iloc[0]
                frequency_by_origin.append((freq, first_trip_id, first_stop_id))
            
            shortest_interval, first_trip_id, first_stop_id = min(frequency_by_origin)
            first_trip_time = first_departure_times[(first_departure_times.route_id == route_id) & (first_departure_times.stop_id == first_stop_id)].departure_time.min() / SECONDS_TO_MINUTES
            last_trip_time = first_departure_times[(first_departure_times.route_id == route_id) & (first_departure_times.stop_id == first_stop_id)].departure_time.max() / SECONDS_TO_MINUTES

            #shift so day starts at 5am
            first_trip_time = (round(first_trip_time) + FIVE_HOURS_IN_MINUTES) % ONE_DAY_IN_MINUTES
            last_trip_time = (round(last_trip_time) + FIVE_HOURS_IN_MINUTES) % ONE_DAY_IN_MINUTES

            for stop_id in stop_times.stop_id[stop_times.trip_id == first_trip_id]:
                routes_frequencies_stops.append([route_id, shortest_interval, first_trip_time, last_trip_time, self.collapsed_stop_mapping[stop_id]])

        self.original_route_info = pd.DataFrame(routes_frequencies_stops, columns=["route_id", "shortest_interval", "first_trip_time", "last_trip_time", "collapsed_stop"]).drop_duplicates()       
        
    def bus_route_weighting_function(self, previous_edges, u, v):
        previous_edge = previous_edges[-1] if previous_edges else None

        driving_time = self.edge_attriutes.driving_time.loc[u,v]
        requires_transfer = False
        if previous_edge is not None:
            previous_edge_routes =  set(self.edge_attriutes.routes.loc[previous_edge])
            current_edge_routes = set(self.edge_attriutes.routes.loc[u,v])
            requires_transfer = len(previous_edge_routes.intersection(current_edge_routes)) > 0
        return driving_time + float(requires_transfer) * TRANSFER_PENALTY_MINUTES * len(previous_edges) * STOP_PENALTY_MINUTES

    def get_route(self, origin: Point, destination: Point):
        origin_stop_mask, _ = util.find_closest(origin, self.feed.stops.geometry)
        origin_stop_idx = self.collapsed_stop_mapping[self.feed.stops.stop_id.loc[origin_stop_mask]]

        destination_stop_mask = util.find_all_within(destination, self.feed.stops.geometry, WALKING_DISTANCE_METERS)
        destination_stops = self.feed.stops.stop_id.loc[destination_stop_mask] 
        destination_stop_idxs = list(set(self.collapsed_stop_mapping[s] for s in destination_stops))

        distance, stop_pair_list = util.multisource_dijkstra(self.G, destination_stop_idxs, origin_stop_idx, weight_function= lambda previous_edge, u,v: self.bus_route_weighting_function(previous_edge, u,v))
        return distance, stop_pair_list

    
    
    
if __name__ == "__main__":
    dataset = RoutePlanDataset("miami", "data/miami/miami_gtfs.zip", save_folder="datasets/miami")
    dataset.build()
    dataset: RoutePlanDataset = RoutePlanDataset.load("datasets/miami")
    dataset.build()
    
    origin = Point(-122.4854634464319, 37.78317831347335)
    destination = Point(-122.40820083305051, 37.71270890747616)

    distance, stop_pair_list = dataset.get_route(origin, destination)
    viz =  util.visualize_route(stop_pair_list, dataset.node_attributes, dataset.edge_attriutes) 
    
    fig, ax = plt.subplots(figsize=(10, 8))
    viz.plot(ax=ax, color=viz.color)
    ctx.add_basemap(ax, crs='EPSG:4326') 
    plt.show()