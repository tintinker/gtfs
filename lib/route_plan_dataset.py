import json
from pathlib import Path
from typing import List, Union
from lib.dataset import Dataset
from lib.bus_plan_state import BusPlanState
import lib.util as util
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

    
    
