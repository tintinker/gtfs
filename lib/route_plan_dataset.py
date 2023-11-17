import json
from pathlib import Path
from typing import List, Union
import numpy as np
from dataset import Dataset
import random
from bus_plan_state import BusPlanState
import util
import contextily as ctx
import matplotlib.pyplot as plt
from shapely import Point



class RoutePlanDataset(Dataset):
    @staticmethod
    def load(save_folder):
        dataset = Dataset.load(save_folder)
        dataset.__class__ = RoutePlanDataset
        return dataset
    
    def save(self, save_folder: Union[str, Path] = None):
        save_folder = Path(save_folder) if save_folder else self.save_folder
        super().save(save_folder)

    
    def build(self, override_if_already_built=False, save_folder: Union[str, Path] = None):
        super()._build(override_if_already_built, save_folder)
        self.built = True
    
    def get_original_bus_plan_state(self) -> BusPlanState:
        if Path(self.save_folder / "original.busplanstate.json").is_file():
            with open(Path(self.save_folder / "original.busplanstate.json")) as f:
                data = json.load(f)
            return BusPlanState("original", self.node_attributes.index.tolist(), self.save_folder, from_json=data)
        bps = BusPlanState.create_from_feed(self.feed, self.collapsed_stop_mapping, self.node_attributes.index.tolist(), self.save_folder)
        bps.save()
        return bps
    
    def get_blank_bus_plan_state(self, name) -> BusPlanState:
        if name == "original":
            raise Exception("You can name it anything but 'original'")
        return BusPlanState(name, self.node_attributes.index.tolist(), self.save_folder)

    def bus_route_weighting_function(self, bus_plan_state: BusPlanState, previous_edge, u, v):
        driving_time = (1/util.AVG_BUS_SPEED_METERS_PER_MIN) * util.approx_manhattan_distance_in_meters(self.node_attributes.geometry.loc[u], self.node_attributes.geometry.loc[v], dataset.cosine_of_longitude) 
        common_routes = bus_plan_state.get_routes_in_common(previous_edge, (u,v))
        requires_transfer = (len(common_routes) == 0)
        return (
            driving_time 
            + util.STOP_PENALTY_MINUTES
            + float(requires_transfer) * bus_plan_state.get_min_wait_time_at_stop(u)
            + float(requires_transfer) * util.TRANSFER_PENALTY_MINUTES 
            + float(not requires_transfer) * min(0, bus_plan_state.get_overall_shortest_interval(common_routes) - bus_plan_state.get_min_wait_time_at_stop(u))
        )
    
    def print_route_info(self, node_pair_list, bus_plan_state: BusPlanState):
        for i in range(len(node_pair_list)):
            u,v = node_pair_list[i]
            previous_edge = node_pair_list[i-1] if i > 0 else None

            driving_time = (1/util.AVG_BUS_SPEED_METERS_PER_MIN) * util.approx_manhattan_distance_in_meters(self.node_attributes.geometry.loc[u], self.node_attributes.geometry.loc[v], dataset.cosine_of_longitude) 
            common_routes = bus_plan_state.get_routes_in_common(previous_edge, (u,v))
            requires_transfer = (len(common_routes) == 0)
            slower_route_adjustment = min(0, bus_plan_state.get_overall_shortest_interval(common_routes) - bus_plan_state.get_min_wait_time_at_stop(u))

            print(self.node_attributes.stop_name.loc[u], "->", self.node_attributes.stop_name.loc[v], ":", end="")
            print("Driving time:", driving_time, "Requires Transfer: ", requires_transfer)
            if requires_transfer:
                print("Penalty:", util.TRANSFER_PENALTY_MINUTES , "Wait time:", bus_plan_state.get_min_wait_time_at_stop(u))
            elif slower_route_adjustment > 0:
                print("Adjustment for slower route to avoid transfer:", slower_route_adjustment)
            else:
                print("Common routes: ", common_routes)
            print()

    def get_route_from_points(self, bps: BusPlanState, origin: Point, destination: Point):
        origin_stop_mask, _ = util.find_closest(origin, self.feed.stops.geometry, self.cosine_of_longitude)
        origin_stop_idx = self.collapsed_stop_mapping[self.feed.stops.stop_id.loc[origin_stop_mask]]

        destination_stop_mask = util.find_all_within(destination, self.feed.stops.geometry, util.WALKING_DISTANCE_METERS, self.cosine_of_longitude)
        destination_stops = self.feed.stops.stop_id.loc[destination_stop_mask] 
        destination_stop_idxs = list(set(self.collapsed_stop_mapping[s] for s in destination_stops))

        return util.multisource_dijkstra(bps.G, destination_stop_idxs, origin_stop_idx, weight_function= lambda previous_edge, u,v: self.bus_route_weighting_function(bps, previous_edge, u,v))

    def get_route_from_stops(self, bps: BusPlanState, origin_stop_idx: int, destination_stop_idxs: List[int]):
        return util.multisource_dijkstra(bps.G, destination_stop_idxs, origin_stop_idx, weight_function= lambda previous_edge, u,v: self.bus_route_weighting_function(bps, previous_edge, u,v))

    
    
if __name__ == "__main__":
    np.random.seed(42)
    random.seed(42)
    # dataset = RoutePlanDataset("miami", "data/miami/miami_gtfs.zip", save_folder="datasets/miami")
    # dataset.build()

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


    

    
    



    
    