import pandas as pd
import json
from zipfile import ZipFile
import numpy as np
from shapely import Point, LineString
import geopandas as gpd
import networkx as nx
import heapq
from urllib.parse import urlencode, urlparse, parse_qs
from urllib.request import urlopen, Request
import random

SECONDS_TO_MINUTES = 60
FIVE_HOURS_IN_MINUTES = 5 * 60
ONE_DAY_IN_MINUTES = 24 * 60
WALKING_DISTANCE_METERS = 400
TRANSFER_PENALTY_MINUTES = 5
STOP_PENALTY_MINUTES = 0.5
DEFAULT_ROUTE_FREQUENCY_IN_MINS = 15
AVG_BUS_SPEED_METERS_PER_MIN = 833 #about 30mph

METERS_TO_DEGREE = 111111 #https://gis.stackexchange.com/questions/2951/algorithm-for-offsetting-a-latitude-longitude-by-some-amount-of-meters
COLORS = ['red', 'blue', 'green', 'purple', 'orange', 'brown', 'pink']

        
def export_json(d, filename):
    with open(filename, "w+") as f:
        json.dump(d, f)


def find_all_within(origin: Point, options: gpd.GeoSeries, distance_in_meters, cosine_of_longitude: float):
    distances = options.apply(lambda d: approx_distance_in_meters(origin, d, cosine_of_longitude))
    mask = distances <= distance_in_meters
    return mask

def find_closest(origin: Point, options: gpd.GeoSeries, cosine_of_longitude: float):
    distances = options.apply(lambda d: approx_distance_in_meters(origin, d, cosine_of_longitude))
    idx = np.argmin(distances)
    return idx, distances[idx]

def approx_distance_in_meters(origin: Point, destination: Point, cosine_of_longitude: float):
    x_dist = cosine_of_longitude * METERS_TO_DEGREE * np.abs(origin.x  - destination.x)
    y_dist =  METERS_TO_DEGREE * np.abs(origin.y  - destination.y)
    return np.sqrt(x_dist ** 2 + y_dist ** 2)

def approx_manhattan_distance_in_meters(origin: Point, destination: Point, cosine_of_longitude: float):
    x_dist = cosine_of_longitude * METERS_TO_DEGREE * np.abs(origin.x  - destination.x)
    y_dist =  METERS_TO_DEGREE * np.abs(origin.y  - destination.y)
    return x_dist + y_dist

def filter_graph(g: nx.Graph, filter_edge = lambda graph, source_node, destination_node: True, filter_node = lambda graph, node: True):
    view = nx.subgraph_view(g, filter_edge=lambda n1, n2: filter_edge(g, n1, n2))
    view2 = nx.subgraph_view(view, filter_node=lambda n: filter_node(view, n))

    if len(view2) == 0:
        return view, False
    return view2, True

def visualize_route(node_pair_list, bps, node_attributes):
    edges_data = []
    for u, v in node_pair_list:
        line = LineString([
            (node_attributes.loc[u]['stop_lon'], node_attributes.loc[u]['stop_lat']),
            (node_attributes.loc[v]['stop_lon'], node_attributes.loc[v]['stop_lat'])
            ])
        edges_data.append({
            'from': node_attributes.loc[u]['stop_name'],
                'to':  node_attributes.loc[v]['stop_name'],
                'routes': bps.get_bus_routes_on_edge((u,v)),
                'geometry': line
            })
    
    current_color = 0
    edges_data[0]["color"] = COLORS[current_color]
    for i in range(1, len(edges_data)):

        if len(edges_data[i]['routes'].intersection(edges_data[i - 1]['routes'])) < 1 :
            current_color = (current_color + 1) % len(COLORS)
        edges_data[i]["color"] = COLORS[current_color]

    gdf_edges = gpd.GeoDataFrame(edges_data, crs="EPSG:4326")
    return gdf_edges



def visualize_bps(bps, node_attributes):
    edges_data = []
    for u, v in bps.G.edges:

        line = LineString([
            (node_attributes.loc[u]['stop_lon'], node_attributes.loc[u]['stop_lat']),
            (node_attributes.loc[v]['stop_lon'], node_attributes.loc[v]['stop_lat'])
            ])
        edges_data.append({
            'from': node_attributes.loc[u]['stop_name'],
                'to':  node_attributes.loc[v]['stop_name'],
                'routes': bps.get_bus_routes_on_edge((u,v)),
                'geometry': line
            })
    current_color = 0
    edges_data[0]["color"] = COLORS[current_color]
    for i in range(1, len(edges_data)):
        if not edges_data[i]['routes'].intersection(edges_data[i - 1]['routes']):
            current_color = (current_color + 1) % len(COLORS)
        edges_data[i]["color"] = COLORS[current_color]

    gdf_edges = gpd.GeoDataFrame(edges_data, crs="EPSG:4326")
    return gdf_edges


def visualize_delay(subgraph: nx.Graph, node_attributes, edge_attributes):
    edges_data = []
    for u, v in subgraph.edges:
        line = LineString([
            (node_attributes.loc[u]['stop_lon'], node_attributes.loc[u]['stop_lat']),
            (node_attributes.loc[v]['stop_lon'], node_attributes.loc[v]['stop_lat'])
            ])
        edges_data.append({
            'from': node_attributes.loc[u]['stop_name'],
                'to':  node_attributes.loc[v]['stop_name'],
                'routes': edge_attributes.loc[u,v]['routes'],
                'avg_delay': edge_attributes.loc[u,v]['avg_delay'] if 'avg_delay' in edge_attributes else np.nan,
                'geometry': line
            })
    gdf_edges = gpd.GeoDataFrame(edges_data, crs="EPSG:4326")
    return gdf_edges


def multisource_dijkstra(G: nx.Graph, sources, target, weight_function: lambda prev_edge, u, v: None):
    distances = {node: np.inf for node in G.nodes}
    paths = {node: [] for node in G.nodes}
    
    # Priority queue to keep track of nodes with their current distance and path
    priority_queue = [(0, source, ()) for source in sources]

    while priority_queue:
        current_distance, current_node, previous_edges = heapq.heappop(priority_queue)

        if current_distance > distances[current_node]:
            continue  # Skip if a shorter path has already been found

        for neighbor in G.neighbors(current_node):
            new_distance = current_distance + weight_function(previous_edges[-1] if previous_edges else None, current_node, neighbor)

            if new_distance < distances[neighbor]:
                distances[neighbor] = new_distance
                new_previous_edges = previous_edges + ((current_node, neighbor),)
                heapq.heappush(priority_queue, (new_distance, neighbor, new_previous_edges))
                paths[neighbor] = new_previous_edges

    final_distance = distances[target]
    final_route = paths[target]

    return final_distance, final_route

def load_gtfs_zip(filename):
    def seconds_since_midnight(time_string):
        try:
            vals = time_string.split(':')
            seconds = 0
            for p, v in enumerate(vals):
                seconds += int(v) * (3600/(60**p))
            return seconds
        except:
            return np.nan
    
    with ZipFile(filename) as myzip:
            data_types = {
                'shape_id': str,
                'stop_id': str,
                'route_id': str,
                'trip_id': str
            }
            stops = pd.read_csv(myzip.open('stops.txt'), dtype=data_types )
            trips = pd.read_csv(myzip.open('trips.txt'), dtype=data_types)
            stop_times = pd.read_csv(myzip.open('stop_times.txt'), dtype=data_types)
            routes = pd.read_csv(myzip.open('routes.txt'), dtype=data_types)

            stops["geometry"] = stops.apply(lambda row: Point(row["stop_lon"], row["stop_lat"]), axis=1)
            stops = gpd.GeoDataFrame(stops, geometry="geometry")

            stop_times.arrival_time = stop_times.arrival_time.apply(seconds_since_midnight)
            stop_times.departure_time = stop_times.departure_time.apply(seconds_since_midnight)

    return stops, trips, stop_times, routes

def format_req(url, key = None):
    if not key:
        return Request(url)
    
    parsed_url = urlparse(url)
    query_parameters = {'api_key': key, 'token': key }
    existing_query_params = parse_qs(parsed_url.query)
    existing_query_params.update(query_parameters)
    new_query = urlencode(existing_query_params, doseq=True)
    new_url = parsed_url._replace(query=new_query).geturl()
    
    req = Request(new_url)
    req.add_header('Authorization', key)
    return req

def seconds_since_midnight(dt):
    midnight = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    delta = dt - midnight
    return delta.total_seconds()