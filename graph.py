import pandas as pd
from sqlalchemy import create_engine
from tqdm import tqdm
import networkx as nx
from gtfs_functions import Feed
from query import make_query
import yaml
import os
import argparse
import json
import random

def main():
    parser = argparse.ArgumentParser(description="Python equivalent of a Bash script")
    parser.add_argument("config_file", help="Configuration file")
    parser.add_argument("-s", "--samples",type=int, default=5, help="Samples per route")

    args = parser.parse_args()

    config_file = args.config_file


    if not os.path.isfile(config_file):
        print(f"Error: Configuration file '{config_file}' not found.")
        exit(1)

    with open(config_file, 'r') as file:
        config_data = yaml.safe_load(file)

    data_dir = config_data.get("data_dir", "data")
    name = config_data.get("name")
    gtfs_filename = config_data.get("gtfs_filename")
    sqlite_db = config_data.get("sqlite_db")

    cache_filename = config_data.get("cache_filename") 
    graph_output_filename =  config_data.get("graph_filename") 

    feed = Feed(gtfs_filename, patterns=False)

    stops = pd.DataFrame(feed.stops.drop(columns='geometry'))
    stops.index = stops.stop_id.astype(str)
    attributes_dict = stops.fillna(0).to_dict(orient='index')

    trips = feed.trips
    stops = pd.DataFrame(feed.stops.drop(columns='geometry'))
    stop_times = pd.DataFrame(feed.stop_times.drop(columns='geometry'))

    sampled_trips = []
    for route_id, group in trips.groupby('route_id'):
        if len(group) >= args.samples:
            sampled_trips.extend(random.sample(group['trip_id'].tolist(), args.samples))
        else:
            sampled_trips.extend(group['trip_id'].tolist())

    conn = create_engine(f"sqlite:///{sqlite_db}")

    df = pd.read_sql_query(make_query(), conn)

    df = df.groupby(['stop_id', 'trip_sequence']).agg({
        'trip_id': 'first',
        'route_id': 'first',
        'stop_name': 'first',
        'route_name': 'first',
        'minute_delay': 'mean',
        'oid': 'max',
        'actual_arrival_time': ['max', 'min'],
        'planned_arrival_time': ['max', 'min']}).reset_index()

    df.columns = df.columns.to_flat_index().map(lambda x: x[0]+"_"+x[1] if x[1] == 'max' or x[1] == 'min' else x[0])
    
    df.to_csv(cache_filename)
    
    G = nx.DiGraph()

    df["stop_id"] = df.stop_id.astype(str)

    # Iterate through the sampled trips
    for trip_id in tqdm(sampled_trips):
        trip_stop_times = stop_times[stop_times['trip_id'] == trip_id]

        # Iterate through the stop times for the current trip
        for _, row in tqdm(trip_stop_times.iterrows(),  total=len(trip_stop_times), leave=False):
            stop_id = row['stop_id']

            if len(trip_stop_times) > 1:
                next_stop = trip_stop_times[trip_stop_times['stop_sequence'] == row['stop_sequence'] + 1]
                if not next_stop.empty:
                    next_stop_id = next_stop['stop_id'].values[0]
                    next_stop_sequence = next_stop['stop_sequence'].values[0]
                    delay_info = df[(df.stop_id == next_stop_id)]
                    if len(delay_info) > 0 and not G.has_edge(stop_id, next_stop_id):
                        G.add_edge(stop_id, next_stop_id, route = trips[trips.trip_id == trip_id].route_id.iloc[0], avg_delay = min(max(delay_info.minute_delay.iloc[0], 0), 30))
                    elif not G.has_edge(stop_id, next_stop_id):
                        G.add_edge(stop_id, next_stop_id, route = trips[trips.trip_id == trip_id].route_id.iloc[0])

    nx.set_node_attributes(G, attributes_dict)

    graph_data = nx.node_link_data(G)

    # Save the JSON data to a file
    with open(graph_output_filename, "w+") as f:
        json.dump(graph_data, f)



if __name__ == '__main__':
    main()