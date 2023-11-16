from ast import literal_eval
import json
import numpy as np
from shapely import Point, LineString
import geopandas as gpd
import networkx as nx
import heapq
import re

       

METERS_TO_DEGREE = 111111 #https://gis.stackexchange.com/questions/2951/algorithm-for-offsetting-a-latitude-longitude-by-some-amount-of-meters
COLORS = ['red', 'blue', 'green', 'purple', 'orange', 'brown', 'pink']


def parse_set_string(s):
        return set([item.replace('"', '') for item in s[1:-1].split()]) if s else set()
        
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

def filter_graph(g: nx.Graph, filter_edge = lambda graph, source_node, destination_node: True, filter_node = lambda graph, node: True):
    view = nx.subgraph_view(g, filter_edge=lambda n1, n2: filter_edge(g, n1, n2))
    view2 = nx.subgraph_view(view, filter_node=lambda n: filter_node(view, n))

    ok = True
    if len(view2) == 0:
        return view, False
    return view2, ok

def visualize_route(node_pair_list, node_attributes, edge_attributes):
    edges_data = []
    for u, v in node_pair_list:
        u = np.int64(u)
        v = np.int64(v)
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
    
    current_color = 0
    edges_data[0]["color"] = COLORS[current_color]
    for i in range(1, len(edges_data)):

        if len(parse_set_string(edges_data[i]['routes']).intersection(parse_set_string(edges_data[i - 1]['routes']))) < 1 :
            current_color = (current_color + 1) % len(COLORS)
        else:
            print(parse_set_string(edges_data[i]['routes']).intersection(parse_set_string(edges_data[i - 1]['routes'])))
        edges_data[i]["color"] = COLORS[current_color]

    gdf_edges = gpd.GeoDataFrame(edges_data, crs="EPSG:4326")
    return gdf_edges

def visualize_routes(subgraph: nx.Graph, node_attributes, edge_attributes):
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
    
    current_color = 0
    edges_data[0]["color"] = COLORS[current_color]
    for i in range(1, len(edges_data)):
        if not edges_data[i].intersection(edges_data[i - 1]):
            current_color = (current_color + 1) % len(COLORS)
        edges_data[i] = current_color

    gdf_edges = gpd.GeoDataFrame(edges_data, crs="EPSG:4326")
    return gdf_edges





def multisource_dijkstra(G: nx.Graph, sources, target, weight_function: lambda prev_edge, u, v: None):
    distances = {node: float('inf') for node in G.nodes}
    paths = {node: [] for node in G.nodes}
    
    # Priority queue to keep track of nodes with their current distance and path
    priority_queue = [(0, source, ()) for source in sources]

    while priority_queue:
        current_distance, current_node, previous_edges = heapq.heappop(priority_queue)

        if current_distance > distances[current_node]:
            continue  # Skip if a shorter path has already been found

        for neighbor in G.neighbors(current_node):
            new_distance = current_distance + weight_function(previous_edges, current_node, neighbor)

            if new_distance < distances[neighbor]:
                distances[neighbor] = new_distance
                new_previous_edges = previous_edges + ((current_node, neighbor),)
                heapq.heappush(priority_queue, (new_distance, neighbor, new_previous_edges))
                paths[neighbor] = new_previous_edges

    final_distance = distances[target]
    final_route = paths[target]

    return final_distance, final_route

