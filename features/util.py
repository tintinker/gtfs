import json
import numpy as np
from shapely import Point
import geopandas as gpd
import networkx as nx

METERS_TO_DEGREE = 111111 #https://gis.stackexchange.com/questions/2951/algorithm-for-offsetting-a-latitude-longitude-by-some-amount-of-meters
NEARBY_STOP_THRESHOLD = 200
NEARBY_POI_THRESHOLD = 400

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

def filter_graph(g: nx.Graph(), filter_edge = lambda graph, source_node, destination_node: True, filter_node = lambda graph, node: True):
    view = nx.subgraph_view(g, filter_edge=lambda n1, n2: filter_edge(g, n1, n2))
    view2 = nx.subgraph_view(view, filter_node=lambda n: filter_node(view, n))

    ok = True
    if len(view2) == 0:
        return view, False
    return view2, ok