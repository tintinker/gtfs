import json
from pathlib import Path
from typing import List, Union
import numpy as np
from lib.dataset import Dataset
import random
from lib.bus_plan_state import BusPlanState
import lib.util as util
import contextily as ctx
import matplotlib.pyplot as plt
from shapely import Point
from lib.route_plan_dataset import RoutePlanDataset

if __name__ == "__main__":
    np.random.seed(42)
    random.seed(42)
    dataset = RoutePlanDataset("sanfrancisco", "data/sanfrancisco/sanfrancisco_gtfs.zip", save_folder="datasets/sanfrancisco")
    dataset.build()
    exit(0)
    dataset: RoutePlanDataset = RoutePlanDataset.load("datasets/miami")

    def show(viz):
        fig, ax = plt.subplots(figsize=(10, 8))
        viz.plot(ax=ax, color=viz.color)
        ctx.add_basemap(ax, crs='EPSG:4326', zoom=10) 
        plt.show()


    stops = dataset.node_attributes
    bus_dependent = (stops.rentor_occupied_units_percent > 0.4) & (stops.vehicles_percent < 0.5) & (stops.income_to_poverty_under_200_percent > 0.2)
    stops["color"] = bus_dependent.apply(lambda row: "red" if row else "blue")
    show(stops)

    def benchmark1(bps: BusPlanState):
        origin = Point(-122.4854634464319, 37.78317831347335)
        destination = Point(-122.40820083305051, 37.71270890747616)

        distance, stop_pair_list = dataset.get_route_from_points(bps, origin, destination)
        if stop_pair_list:
            print("Distance: ", distance)
            viz =  util.visualize_route(stop_pair_list, bps, dataset.node_attributes) 
            dataset.print_route_info(stop_pair_list, bps)
            show(viz)
            return distance
        else:
            print("No route found")
            return -np.inf
    
    def benchmark2(bps: BusPlanState):
        origin = random.choice(dataset.node_attributes[bus_dependent].index.tolist())
        destinations = dataset.node_attributes[dataset.node_attributes.near_hospital].index.tolist()

        distance, stop_pair_list = dataset.get_route_from_stops(bps, origin, destinations)
        if stop_pair_list:
            print("Distance: ", distance)
            viz =  util.visualize_route(stop_pair_list, bps, dataset.node_attributes) 
            dataset.print_route_info(stop_pair_list, bps)
            show(viz)
            return distance
        else:
            print("No route found")
            return -np.inf

    original_bus_plan_state = dataset.get_original_bus_plan_state()
    
    viz = util.visualize_bps(original_bus_plan_state, dataset.node_attributes)
    show(viz)


    random_circle_bps = dataset.get_blank_bus_plan_state("random_circle")
    
    random_bus_dependent_stops = random.sample(dataset.node_attributes.index[bus_dependent].tolist(), k=25)
    random_general_stops = random.sample(dataset.node_attributes.index.tolist(), k=25)
    for collapsed_stop_idx in (random_bus_dependent_stops + random_general_stops):
        nearby_stops = util.find_all_within(dataset.node_attributes.geometry.loc[collapsed_stop_idx], dataset.node_attributes.geometry, 2 * 1600, dataset.cosine_of_longitude)
        for nearby_stop in dataset.node_attributes.loc[nearby_stops].index:
            random_circle_bps.add_stop_to_current_route(nearby_stop)
        random_circle_bps.end_current_route()

    viz = util.visualize_bps(random_circle_bps, dataset.node_attributes)
    show(viz)

    original_plan_score = benchmark1(original_bus_plan_state) + benchmark2(original_bus_plan_state)
    
    random_circle_score = benchmark1(random_circle_bps) + benchmark2(random_circle_bps)

    print("original score:", original_plan_score)
    print("random circle score:", random_circle_score)


    

    
    



    
    