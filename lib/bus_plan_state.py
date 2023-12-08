from collections import defaultdict
import json
from pathlib import Path
from tqdm import tqdm
import lib.util as util
import pandas as pd
import networkx as nx
import numpy as np

class BusPlanState:
    def __init__(self, name: str, node_attributes: pd.DataFrame, save_folder: str, from_json = None):
        self.name = name
        self.node_index_list = node_attributes.index.tolist()
        self.node_attributes = node_attributes
        self.save_folder = save_folder
        
        self.shortest_intervals = {}
        self.routes_to_stops = defaultdict(list)
        self.stops_to_routes = defaultdict(set)
        self.G = nx.DiGraph()
        self.G.add_nodes_from(self.node_index_list)


        if from_json is not None:
            self._load_json(from_json)
        else:
            self.current_route = 0
            self.shortest_intervals[self.current_route] = util.DEFAULT_ROUTE_FREQUENCY_IN_MINS
        self.stops_to_routes = {int(k):v for k,v in self.stops_to_routes.items()}

    def __str__(self) -> str:
        return f"BusPlanState: {len(self.routes)} routes, {len(self.stops)} stops, graph: {self.G}"
    
    def __eq__(self, other) -> bool:
        if not isinstance(other, BusPlanState):
            return False
        if self.shortest_intervals != other.shortest_intervals:
            return False
        if self.routes_to_stops != other.routes_to_stops:
            return False
        if self.stops_to_routes != other.stops_to_routes:
            return False
        if self.G.edges != other.G.edges:
            return False
        return True
    
    def route_length_similarity_example(self, other):
        def route_length_info(bps: BusPlanState):
            route_lengths = [len(v) for v in bps.routes_to_stops.values()]
            return np.mean(route_lengths), np.std(route_lengths), len(bps.routes_to_stops.keys()) #avg, standard dev, and number of routes
        
        self_route_length_info = np.array(route_length_info(self))
        other_route_length_info = np.array(route_length_info(other))
       
        distance = np.linalg.norm(self_route_length_info - other_route_length_info) #distance = sqrt(a^2 + b^2 + c^2 + ...)
        similarity = 1 / distance
        return similarity
        
    
    def node_attribute_similarity_example(self, other):
        def lat_lng_grocery_info(bps: BusPlanState):
            indices = np.array(bps.all_stops)
            #full list of attributes in datasets/[CITY]/node_attributes.csv
            latitudes = bps.node_attributes.stop_lat.loc[indices]
            longitudes = bps.node_attributes.stop_lon.loc[indices]
            near_grocery = bps.node_attributes.near_grocery.loc[indices] #near grocery is a binary 1 if next to grocery, 0 if not next to grocery store
            return np.mean(latitudes), np.std(latitudes), np.mean(longitudes), np.std(longitudes), np.mean(near_grocery)
        
        self_lat_lng_grocery = np.array(lat_lng_grocery_info(self))
        other_lat_lng_grocery = np.array(lat_lng_grocery_info(other))

        distance = np.linalg.norm(self_lat_lng_grocery - other_lat_lng_grocery) #distance = sqrt(a^2 + b^2 + c^2 + ...)
        similarity = 1 / distance
        return similarity
    
    def current_route_similarity_example(self, other):
        def current_route_info(bps: BusPlanState, route_index):
            stops = bps.routes_to_stops[route_index]
            if not stops:
                return 0, 0, 0, 0, 0
            
            num_stops_on_route = len(stops)
            first_latitude, first_longitude = bps.node_attributes.stop_lat.loc[stops[0]], bps.node_attributes.stop_lon.loc[stops[0]]
            last_latitude, last_longitude = bps.node_attributes.stop_lat.loc[stops[-1]], bps.node_attributes.stop_lon.loc[stops[-1]]   
            return num_stops_on_route, last_latitude, last_longitude, np.abs(first_latitude - last_latitude), np.abs(first_longitude - last_longitude)
        
        self_current_route_info = current_route_info(self, self.current_route)
        
        #if we're on the third route of the current bus plan, compare the third route of other bus plans
        if self.current_route in other.routes_to_stops:
            other_current_route_info = current_route_info(other, self.current_route)
        #if comparing with the original bus plan for a city, their routes will be strings, so need to find the string at the correct index
        elif self.current_route <= len(other.routes_to_stops.keys()):
            other_current_route_info = current_route_info(other,  list(other.routes_to_stops.keys())[-1])
        #if the other bus plan doesn't have enough routes, just take the last one
        else:
            other_current_route_info = current_route_info(other, list(other.routes_to_stops.keys())[-1]) 
            

        distance = np.linalg.norm(np.array(self_current_route_info) - np.array(other_current_route_info)) #distance = sqrt(a^2 + b^2 + c^2 + ...)
        similarity = 1 / distance
        return similarity
        
    
    def _load_json(self, json_dict):
        self.shortest_intervals = json_dict["shortest_intervals"]
        self.routes_to_stops.update(json_dict["routes_to_stops"])
        self.stops_to_routes.update({k:set(v) for k,v in json_dict["stops_to_routes"].items()})
        self.G = nx.node_link_graph(json_dict["graph"])

    def save(self):
        data = {
            "shortest_intervals": self.shortest_intervals,
            "routes_to_stops": self.routes_to_stops,
            "stops_to_routes": {k:list(v) for k, v in self.stops_to_routes.items()},
            "graph": nx.node_link_data(self.G)
        }
        with open(Path(self.save_folder) / (self.name + ".busplanstate.json"), "w+") as f:
            json.dump(data, f)
   
    def add_stop_to_current_route(self, stop_id):
        if stop_id not in self.node_index_list:
            raise Exception(f"Couldn't find that stop: {stop_id}")
        
        self.routes_to_stops[self.current_route].append(stop_id)
        self.stops_to_routes[stop_id].add(self.current_route)

        if len(self.routes_to_stops[self.current_route]) > 1:
            self.G.add_edge(self.routes_to_stops[self.current_route][-2], self.routes_to_stops[self.current_route][-1])
    
    def end_current_route(self):
        self.current_route += 1
        self.shortest_intervals[self.current_route] = util.DEFAULT_ROUTE_FREQUENCY_IN_MINS

    def change_frequency_for_route(self, route, shortest_interval):
        if route > self.current_route:
            raise Exception("We haven't added that many routes yet!")
        self.shortest_intervals[route] = shortest_interval
    
    
    def get_bus_routes_on_edge(self, edge):
        u,v = edge
        return self.stops_to_routes[u].intersection(self.stops_to_routes[v])

    def get_routes_in_common(self, edge1, edge2):
        if not edge1:
            return set()
        return self.get_bus_routes_on_edge(edge1).intersection(self.get_bus_routes_on_edge(edge2))

    def get_requires_transfer(self, edge1, edge2):
        if not edge1:
            return True
        return len(self.get_routes_in_common(edge1, edge2)) == 0
    
    def get_overall_shortest_interval(self, routes):
        if not routes:
            return 0
        return min([self.shortest_intervals[r] for r in routes])

    def get_min_wait_time_at_stop(self, stop_id):
        return min([self.shortest_intervals[r] for r in self.stops_to_routes[stop_id]])
    
    def get_avg_wait_time_for_route(self, route_id):
        return self.shortest_intervals[route_id]
    
    @property
    def all_stops(self):
        """
        every stop from every route, includes duplicates
        """
        return [stop for stop_list in self.routes_to_stops.values() for stop in stop_list]


    @staticmethod
    def create_from_feed(gtfs_zip_file, node_attributes, save_folder):
        shortest_intervals = {}
        routes_to_stops = defaultdict(list)
        stops_to_routes = defaultdict(set)
        G = nx.DiGraph()
    
        stops, trips, stop_times, routes = util.load_gtfs_zip(gtfs_zip_file)
        stop_times = stop_times.merge(trips[["trip_id","route_id"]], on="trip_id",how="left")
        stop_times = stop_times[['route_id', 'trip_id', 'stop_id', 'stop_sequence', 'departure_time']]
        stop_times = stop_times.merge(stops[["stop_id", "stop_name"]], on="stop_id",how="left")
        first_departure_times = stop_times[stop_times.stop_sequence == 1].sort_values('departure_time')
        for route_id in tqdm(routes.route_id.unique()):
            frequency_by_origin = []
            route_times = first_departure_times[first_departure_times.route_id == route_id]
            for first_stop_id in route_times.stop_id.unique():
                freq = route_times[route_times.stop_id == first_stop_id].departure_time.drop_duplicates().diff().median() / util.SECONDS_TO_MINUTES
                first_trip_id = route_times[route_times.stop_id == first_stop_id].trip_id.iloc[0]
                frequency_by_origin.append((freq, first_trip_id, first_stop_id))
            
            if not frequency_by_origin:
                continue
            
            shortest_interval, first_trip_id, first_stop_id = min(frequency_by_origin)
            first_trip_time = first_departure_times[(first_departure_times.route_id == route_id) & (first_departure_times.stop_id == first_stop_id)].departure_time.min() / util.SECONDS_TO_MINUTES
            last_trip_time = first_departure_times[(first_departure_times.route_id == route_id) & (first_departure_times.stop_id == first_stop_id)].departure_time.max() / util.SECONDS_TO_MINUTES

            #shift so day starts at 5am
            first_trip_time = (round(first_trip_time) + util.FIVE_HOURS_IN_MINUTES) % util.ONE_DAY_IN_MINUTES
            last_trip_time = (round(last_trip_time) + util.FIVE_HOURS_IN_MINUTES) % util.ONE_DAY_IN_MINUTES

            routes_to_stops[route_id] = []
            
            for i, row in stop_times[stop_times.trip_id == first_trip_id].sort_values("stop_sequence").iterrows():
                stop_id = row["stop_id"]
                stop_sequence = row["stop_sequence"]

                shortest_intervals[route_id] = shortest_interval
                if stop_id not in routes_to_stops[route_id]:
                    routes_to_stops[route_id].append(stop_id)

                if len(routes_to_stops[route_id]) > 1:
                    G.add_edge(routes_to_stops[route_id][-2], routes_to_stops[route_id][-1])

                stops_to_routes[stop_id].add(route_id)
               

        data = {
            "shortest_intervals": shortest_intervals,
            "routes_to_stops": routes_to_stops,
            "stops_to_routes": {k:list(v) for k, v in stops_to_routes.items()},
            "graph": nx.node_link_data(G)
        }      
        bps = BusPlanState("original", node_attributes, save_folder, from_json = data)
        return bps

   