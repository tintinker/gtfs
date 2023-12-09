import json
from pathlib import Path
from typing import List, Union
from lib.dataset import Dataset
from lib.bus_plan_state import BusPlanState
import lib.util as util
from shapely import Point
import logging


class RoutePlanDataset(Dataset):
    @staticmethod
    def load(save_folder):
        dataset = Dataset.load(save_folder)
        dataset.__class__ = RoutePlanDataset
        return dataset
    
    def save(self, save_folder: Union[str, Path] = None):
        save_folder = Path(save_folder) if save_folder else self.save_folder
        super().save(save_folder)

    
    def build(self, override_if_already_built = False, use_cache = True, save_folder: Union[str, Path] = None):
        super()._build(override_if_already_built, use_cache, save_folder)
        self.built = True
    
    def get_original_bus_plan_state(self) -> BusPlanState:
        if Path(self.save_folder / "original.busplanstate.json").is_file():
            with open(Path(self.save_folder / "original.busplanstate.json")) as f:
                data = json.load(f)
            return BusPlanState("original", self.node_attributes, self.cosine_latitude, self.save_folder, from_json=data)
        bps = BusPlanState.create_from_feed(self.gtfs_source, self.node_attributes, self.cosine_latitude, self.save_folder)
        bps.save()
        return bps
    
    def load_bus_plan_state(self, name, filename) -> BusPlanState:
        with open(filename) as f:
            data = json.load(f)
        return BusPlanState(name, self.node_attributes, self.cosine_latitude, self.save_folder, from_json=data)
    
    def get_blank_bus_plan_state(self, name) -> BusPlanState:
        if name == "original":
            raise Exception("You can name it anything but 'original'")
        return BusPlanState(name, self.node_attributes, self.cosine_latitude, self.save_folder)

    def bus_route_weighting_function(self, bus_plan_state: BusPlanState, previous_edge, u, v):
        driving_time = (1/util.AVG_BUS_SPEED_METERS_PER_MIN) * util.approx_manhattan_distance_in_meters(self.node_attributes.geometry.loc[u], self.node_attributes.geometry.loc[v], self.cosine_latitude) 
        common_routes = bus_plan_state.get_routes_in_common(previous_edge, (u,v))
        requires_transfer = (len(common_routes) == 0)
        return (
            driving_time 
            + util.STOP_PENALTY_MINUTES
            + float(requires_transfer) * bus_plan_state.get_min_wait_time_at_stop(u)
            + float(requires_transfer) * util.TRANSFER_PENALTY_MINUTES 
            + float(not requires_transfer) * min(0, bus_plan_state.get_overall_shortest_interval(common_routes) - bus_plan_state.get_min_wait_time_at_stop(u))
        )
    
    def route_info_string(self, node_pair_list, bus_plan_state: BusPlanState):
        result = ""
        def print_to_result(*args, end="\n"):
            nonlocal result
            for a in args:
                result += str(a) + " "
            result += end

        current_stop = None
        previous_stop = None
        stop_count = 0
        total_driving_time = 0
        all_possible_routes = set()

        for i in range(len(node_pair_list)):
            u,v = node_pair_list[i]
            previous_edge = node_pair_list[i-1] if i > 0 else None

            driving_time = (1/util.AVG_BUS_SPEED_METERS_PER_MIN) * util.approx_manhattan_distance_in_meters(self.node_attributes.geometry.loc[u], self.node_attributes.geometry.loc[v], self.cosine_latitude) 
            common_routes = bus_plan_state.get_routes_in_common(previous_edge, (u,v))
            requires_transfer = (len(common_routes) == 0)
            slower_route_adjustment = min(0, bus_plan_state.get_overall_shortest_interval(common_routes) - bus_plan_state.get_min_wait_time_at_stop(u))

            if requires_transfer and current_stop is None:
                current_stop = self.node_attributes.stop_name.loc[u]
                stop_count = 0
                total_driving_time = 0
                all_possible_routes = bus_plan_state.get_bus_routes_on_edge((u,v))
            elif requires_transfer:
                previous_stop = current_stop
                current_stop = self.node_attributes.stop_name.loc[v]
                print_to_result(previous_stop, "->", current_stop, "(", stop_count, "stops)", ":", end="")
                print_to_result("Driving time:", total_driving_time, "Bus stopping time:", util.STOP_PENALTY_MINUTES * stop_count, "Requires Transfer: ", requires_transfer)
                print_to_result("Possible Routes:", all_possible_routes)
                all_possible_routes = bus_plan_state.get_bus_routes_on_edge((u,v))
                stop_count = 0
                total_driving_time = 0
            else:
                stop_count += 1
                total_driving_time += driving_time
                all_possible_routes = all_possible_routes.intersection(bus_plan_state.get_bus_routes_on_edge((u,v)))

            if requires_transfer:
                print_to_result("Penalty:", util.TRANSFER_PENALTY_MINUTES , "Wait time:", bus_plan_state.get_min_wait_time_at_stop(u))
            elif slower_route_adjustment > 0:
                print_to_result("Adjustment for slower route to avoid transfer:", slower_route_adjustment)
        
        previous_stop = current_stop
        current_stop = self.node_attributes.stop_name.loc[v]
        print_to_result(previous_stop, "->", current_stop, "(", stop_count, "stops)", ":", end="")
        print_to_result("Driving time:", total_driving_time, "Bus stopping time:", util.STOP_PENALTY_MINUTES * stop_count, "Requires Transfer: ", requires_transfer)
        print_to_result("Possible Routes:", all_possible_routes)
        return result

    def get_route_from_points(self, bps: BusPlanState, origin: Point, destination: Point):
        origin_stop_mask, _ = util.find_closest(origin, self.stops_data.geometry, self.cosine_latitude)
        origin_stop_idx = self.stops_data.index.loc[origin_stop_mask]

        destination_stop_mask = util.find_all_within(destination, self.stops_data.geometry, util.WALKING_DISTANCE_METERS, self.cosine_latitude)
        destination_stop_idxs = self.stops_data.index.loc[destination_stop_mask]

        return util.multisource_dijkstra(bps.G, destination_stop_idxs, origin_stop_idx, weight_function= lambda previous_edge, u,v: self.bus_route_weighting_function(bps, previous_edge, u,v))

    def get_route_from_stops(self, bps: BusPlanState, origin_stop_id: str, destination_stop_ids: List[str]):
        return util.multisource_dijkstra(bps.G, destination_stop_ids, origin_stop_id, weight_function= lambda previous_edge, u,v: self.bus_route_weighting_function(bps, previous_edge, u,v))

    
    
