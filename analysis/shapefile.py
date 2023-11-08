import networkx as nx
import geopandas as gpd
from shapely.geometry import Point, LineString
import json
import random
import argparse
import os
import yaml
import pandas as pd
import numpy as np

def main():
    parser = argparse.ArgumentParser(description="Python equivalent of a Bash script")
    parser.add_argument("config_file", help="Configuration file")
    parser.add_argument("-s", "--samples",type=int, default=3, help="Samples per route")

    args = parser.parse_args()

    config_file = args.config_file


    if not os.path.isfile(config_file):
        print(f"Error: Configuration file '{config_file}' not found.")
        exit(1)

    with open(config_file, 'r') as file:
        config_data = yaml.safe_load(file)

    cache_df = pd.read_csv(config_data.get("cache_filename"), header=0)
    graph_output_filename =  config_data.get("graph_filename") 
    shp_folder = config_data.get("shp_folder") 

    os.makedirs(shp_folder, exist_ok=True)

    with open(graph_output_filename) as f:
        g = json.load(f)
    G = nx.node_link_graph(g)


    routes = random.sample(list(cache_df.route_id.unique()), min(args.samples, len(cache_df.route_id.unique())))

    def filter_edge(n1, n2):
        return G[n1][n2].get("route", 0) in routes

    view = nx.subgraph_view(G, filter_edge=filter_edge)

    def filter_node(n1):
        return view.degree(n1) > 0

    view2 = nx.subgraph_view(view, filter_node=filter_node)
    
    if len(view2) == 0:
        view2 = nx.subgraph_view(G, filter_node=lambda n: G.degree[n] > 0)

    # Create a GeoDataFrame from the edges
    edges_data = []
    for u, v, data in view2.edges(data=True):
        avg_delay = data['avg_delay'] if 'avg_delay' in data else np.nan
        line = LineString([(G.nodes[u]['stop_lon'], G.nodes[u]['stop_lat']),
                        (G.nodes[v]['stop_lon'], G.nodes[v]['stop_lat'])])
        edges_data.append({'from': G.nodes[u]['stop_name'], 'to':  G.nodes[v]['stop_name'], 'route': data['route'], 'avg_delay': avg_delay, 'geometry': line})

    gdf_edges = gpd.GeoDataFrame(edges_data, crs="EPSG:4326")

    gdf_edges.to_file(os.path.join(shp_folder, "edges.shp"))

main()