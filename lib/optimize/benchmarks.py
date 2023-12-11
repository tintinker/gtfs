
import numpy as np
from lib.bus_plan_state import BusPlanState
import lib.util as util
import logging

logging.basicConfig(level=logging.DEBUG)


class Benchmarks:
    def __init__(self, dataset, logger=None) -> None:
        self.dataset = dataset
        self.stops = self.dataset.node_attributes
        self.bus_dependent = (self.stops.rentor_occupied_units_percent > 0.4) & (self.stops.vehicles_percent < 0.5) & (self.stops.income_to_poverty_under_200_percent > 0.2)
        self.stops["color"] = self.bus_dependent.apply(lambda row: "red" if row else "blue")

        self.logger = logger
        if not self.logger:
            self.logger = logging.getself.logger(f"benchmarks")
            self.logger.setLevel(logging.DEBUG)
            handler = logging.FileHandler("benchmarks.log")
            self.logger.addHandler(handler)
        

    def benchmark_hospital(self, bps: BusPlanState):
        origin = np.random.choice([o for o in self.dataset.node_attributes[self.bus_dependent].index.tolist() if o in bps.G])
        destinations = [d for d in self.dataset.node_attributes[self.dataset.node_attributes.near_hospital].index.tolist()if d in bps.G]

        self.logger.debug(f"{bps.node_attributes.loc[origin].stop_name} to HOSPITALS {bps.node_attributes.loc[destinations].stop_name.tolist()}")
        distance, stop_pair_list = self.dataset.get_route_from_stops(bps, origin, destinations)
        if stop_pair_list:
            self.logger.debug("Distance: ", distance)
            self.logger.debug(self.dataset.route_info_string(stop_pair_list, bps).strip())
            return distance, stop_pair_list
        else:
            self.logger.debug("No route found")
            return util.NO_ROUTE_PENALTY, []

    def benchmark_park(self, bps: BusPlanState):
        origin = np.random.choice([o for o in self.dataset.node_attributes[self.bus_dependent].index.tolist() if o in bps.G])
        destinations = [d for d in self.dataset.node_attributes[self.dataset.node_attributes.near_park].index.tolist()if d in bps.G]

        self.logger.debug(f"{bps.node_attributes.loc[origin].stop_name} to PARKS {bps.node_attributes.loc[destinations].stop_name.tolist()}")
        distance, stop_pair_list = self.dataset.get_route_from_stops(bps, origin, destinations)
        if stop_pair_list:
            self.logger.debug("Distance: ", distance)
            self.logger.debug(self.dataset.route_info_string(stop_pair_list, bps).strip())
            return distance, stop_pair_list
        else:
            self.logger.debug("No route found")
            return util.NO_ROUTE_PENALTY, []

    def benchmark_grocery(self, bps: BusPlanState):
        origin = np.random.choice([o for o in self.dataset.node_attributes[self.bus_dependent].index.tolist() if o in bps.G])
        destinations = [d for d in self.dataset.node_attributes[self.dataset.node_attributes.near_grocery].index.tolist()if d in bps.G]

        self.logger.debug(f"{bps.node_attributes.loc[origin].stop_name} to GROCERY {bps.node_attributes.loc[destinations].stop_name.tolist()}")
        distance, stop_pair_list = self.dataset.get_route_from_stops(bps, origin, destinations)
        if stop_pair_list:
            self.logger.debug("Distance: ", distance)
            self.logger.debug(self.dataset.route_info_string(stop_pair_list, bps).strip())
            return distance, stop_pair_list
        else:
            self.logger.debug("No route found")
            return util.NO_ROUTE_PENALTY, []
        
    def benchmark_worship(self, bps: BusPlanState):
        origin = np.random.choice([o for o in self.dataset.node_attributes[self.bus_dependent].index.tolist() if o in bps.G])
        destinations = [d for d in self.dataset.node_attributes[self.dataset.node_attributes.near_worship].index.tolist()if d in bps.G]

        self.logger.debug(f"{bps.node_attributes.loc[origin].stop_name} to WORSHIP {bps.node_attributes.loc[destinations].stop_name.tolist()}")
        distance, stop_pair_list = self.dataset.get_route_from_stops(bps, origin, destinations)
        if stop_pair_list:
            self.logger.debug("Distance: ", distance)
            self.logger.debug(self.dataset.route_info_string(stop_pair_list, bps).strip())
            return distance, stop_pair_list
        else:
            self.logger.debug("No route found")
            return util.NO_ROUTE_PENALTY, []

    def benchmark_any_bar(self, bps: BusPlanState):
        origin = np.random.choice([o for o in self.dataset.node_attributes[self.bus_dependent].index.tolist() if o in bps.G])
        destinations = [d for d in self.dataset.node_attributes[self.dataset.node_attributes.near_bar].index.tolist()if d in bps.G]

        self.logger.debug(f"{bps.node_attributes.loc[origin].stop_name} to BAR {bps.node_attributes.loc[destinations].stop_name.tolist()}")
        distance, stop_pair_list = self.dataset.get_route_from_stops(bps, origin, destinations)
        if stop_pair_list:
            self.logger.debug("Distance: ", distance)
            self.logger.debug(self.dataset.route_info_string(stop_pair_list, bps).strip())
            return distance, stop_pair_list
        else:
            self.logger.debug("No route found")
            return util.NO_ROUTE_PENALTY, []
        
    def benchmark_specific_bar(self, bps: BusPlanState):
        origin = np.random.choice([o for o in self.dataset.node_attributes[self.bus_dependent].index.tolist() if o in bps.G])
        
        rng = np.random.RandomState(2)
        destination = rng.choice([d for d in self.dataset.node_attributes[self.dataset.node_attributes.near_bar].index.tolist()if d in bps.G])

        self.logger.debug(f"{bps.node_attributes.loc[origin].stop_name} to SPECIFIC BAR {bps.node_attributes.loc[destination].stop_name}")
        distance, stop_pair_list = self.dataset.get_route_from_stops(bps, origin, [destination])
        if stop_pair_list:
            self.logger.debug("Distance: ", distance)
            self.logger.debug(self.dataset.route_info_string(stop_pair_list, bps).strip())
            return distance, stop_pair_list
        else:
            self.logger.debug("No route found")
            return util.NO_ROUTE_PENALTY, []
        
    def benchmark_starbucks(self, bps: BusPlanState):
        origin = np.random.choice([o for o in self.dataset.node_attributes[self.bus_dependent].index.tolist() if o in bps.G])
        
        rng = np.random.RandomState(2)
        destination = rng.choice([d for d in self.dataset.node_attributes[self.dataset.node_attributes.near_starbucks].index.tolist()if d in bps.G])

        self.logger.debug(f"{bps.node_attributes.loc[origin].stop_name} to STARBUCKS {bps.node_attributes.loc[destination].stop_name}")
        distance, stop_pair_list = self.dataset.get_route_from_stops(bps, origin, [destination])
        if stop_pair_list:
            self.logger.debug("Distance: ", distance)
            self.logger.debug(self.dataset.route_info_string(stop_pair_list, bps).strip())
            return distance, stop_pair_list
        else:
            self.logger.debug("No route found")
            return util.NO_ROUTE_PENALTY, []


    def benchmark_mcdonalds(self, bps: BusPlanState):
        origin = np.random.choice([o for o in self.dataset.node_attributes[self.bus_dependent].index.tolist() if o in bps.G])
        
        rng = np.random.RandomState(2)
        destination = rng.choice([d for d in self.dataset.node_attributes[self.dataset.node_attributes.near_mcdonalds].index.tolist()if d in bps.G])

        self.logger.debug(f"{bps.node_attributes.loc[origin].stop_name} to McD {bps.node_attributes.loc[destination].stop_name}")
        distance, stop_pair_list = self.dataset.get_route_from_stops(bps, origin, [destination])
        if stop_pair_list:
            self.logger.debug("Distance: ", distance)
            self.logger.debug(self.dataset.route_info_string(stop_pair_list, bps).strip())
            return distance, stop_pair_list
        else:
            self.logger.debug("No route found")
            return util.NO_ROUTE_PENALTY, []

    def all_benchmarks(self, bps, times=2):
        total_score = 0
        all_stop_pair_lsts = []
        essentials = [self.benchmark_hospital, self.benchmark_park, self.benchmark_grocery, self.benchmark_worship, self.benchmark_any_bar]
        specific_destinations = [self.benchmark_specific_bar, self.benchmark_starbucks, self.benchmark_mcdonalds]
        lst = essentials + essentials + specific_destinations
        for _ in range(times):
            for benchmark in lst:
                score, stop_pair_lst = benchmark(bps)
                total_score += score
                all_stop_pair_lsts.append(stop_pair_lst)

                self.logger.debug("\n\n\n-----------------------------")
                self.logger.debug(f"{benchmark.__name__}: {score}")
                self.logger.debug("-----------------------------\n\n\n")
        return total_score / (times * len(lst)), all_stop_pair_lsts