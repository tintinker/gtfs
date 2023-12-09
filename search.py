from pathlib import Path
import numpy as np
import random
from lib.bus_plan_state import BusPlanState
from lib.route_plan_dataset import RoutePlanDataset
import lib.util as util
import contextily as ctx
import matplotlib.pyplot as plt
import logging
from tqdm import tqdm
import copy

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(f"search")
logger.setLevel(logging.INFO)
handler = logging.FileHandler("search.log")
logger.addHandler(handler)

def show(viz):
    fig, ax = plt.subplots(figsize=(10, 8))
    viz.plot(ax=ax, color=viz.color)
    ctx.add_basemap(ax, crs='EPSG:4326', zoom=10)
    plt.show()

dataset = RoutePlanDataset.load("datasets/cleveland")

stops = dataset.node_attributes
bus_dependent = (stops.rentor_occupied_units_percent > 0.4) & (stops.vehicles_percent < 0.5) & (stops.income_to_poverty_under_200_percent > 0.2)
stops["color"] = bus_dependent.apply(lambda row: "red" if row else "blue")

def benchmark_hospital(bps: BusPlanState):
    origin = np.random.choice([o for o in dataset.node_attributes[bus_dependent].index.tolist() if o in bps.G])
    destinations = [d for d in dataset.node_attributes[dataset.node_attributes.near_hospital].index.tolist()if d in bps.G]

    logger.debug(f"{bps.node_attributes.loc[origin].stop_name} to HOSPITALS {bps.node_attributes.loc[destinations].stop_name.tolist()}")
    distance, stop_pair_list = dataset.get_route_from_stops(bps, origin, destinations)
    if stop_pair_list:
        logger.debug("Distance: ", distance)
        # viz =  util.visualize_route(stop_pair_list, bps, dataset.node_attributes)
        logger.debug(dataset.route_info_string(stop_pair_list, bps).strip())
        # show(viz)
        return distance
    else:
        logger.debug("No route found")
        return util.NO_ROUTE_PENALTY

def benchmark_park(bps: BusPlanState):
    origin = np.random.choice([o for o in dataset.node_attributes[bus_dependent].index.tolist() if o in bps.G])
    destinations = [d for d in dataset.node_attributes[dataset.node_attributes.near_park].index.tolist()if d in bps.G]

    logger.debug(f"{bps.node_attributes.loc[origin].stop_name} to PARKS {bps.node_attributes.loc[destinations].stop_name.tolist()}")
    distance, stop_pair_list = dataset.get_route_from_stops(bps, origin, destinations)
    if stop_pair_list:
        logger.debug("Distance: ", distance)
        # viz =  util.visualize_route(stop_pair_list, bps, dataset.node_attributes)
        logger.debug(dataset.route_info_string(stop_pair_list, bps).strip())
        # show(viz)
        return distance
    else:
        logger.debug("No route found")
        return util.NO_ROUTE_PENALTY

def benchmark_starbucks(bps: BusPlanState):
    origin = np.random.choice([o for o in dataset.node_attributes[bus_dependent].index.tolist() if o in bps.G])
    
    rng = np.random.RandomState(2)
    destination = rng.choice([d for d in dataset.node_attributes[dataset.node_attributes.near_starbucks].index.tolist()if d in bps.G])

    logger.debug(f"{bps.node_attributes.loc[origin].stop_name} to STARBUCKS {bps.node_attributes.loc[destination].stop_name}")
    distance, stop_pair_list = dataset.get_route_from_stops(bps, origin, [destination])
    if stop_pair_list:
        logger.debug("Distance: ", distance)
        # viz =  util.visualize_route(stop_pair_list, bps, dataset.node_attributes)
        logger.debug(dataset.route_info_string(stop_pair_list, bps).strip())
        # show(viz)
        return distance
    else:
        logger.debug("No route found")
        return util.NO_ROUTE_PENALTY


def benchmark_mcdonalds(bps: BusPlanState):
    origin = np.random.choice([o for o in dataset.node_attributes[bus_dependent].index.tolist() if o in bps.G])
    
    rng = np.random.RandomState(2)
    destination = rng.choice([d for d in dataset.node_attributes[dataset.node_attributes.near_mcdonalds].index.tolist()if d in bps.G])

    logger.debug(f"{bps.node_attributes.loc[origin].stop_name} to McD {bps.node_attributes.loc[destination].stop_name}")
    distance, stop_pair_list = dataset.get_route_from_stops(bps, origin, [destination])
    if stop_pair_list:
        logger.debug("Distance: ", distance)
        # viz =  util.visualize_route(stop_pair_list, bps, dataset.node_attributes)
        logger.debug(dataset.route_info_string(stop_pair_list, bps).strip())
        # show(viz)
        return distance
    else:
        logger.debug("No route found")
        return util.NO_ROUTE_PENALTY

def all_benchmarks(bps, times=5):
    total_score = 0
    for _ in range(times):
        for benchmark in [benchmark_hospital, benchmark_park, benchmark_starbucks, benchmark_mcdonalds]:
            score =  benchmark(bps)
            total_score += score
            logger.debug("\n\n\n-----------------------------")
            logger.debug(f"{benchmark.__name__}: {score}")
            logger.debug("-----------------------------\n\n\n")
    return total_score / (4 * times)

original_bus_plan_state = dataset.get_original_bus_plan_state()

viz = util.visualize_bps(original_bus_plan_state, dataset.node_attributes)
#show(viz)

logger.info(f"original valid: {original_bus_plan_state.valid}")
logger.info(f"original total time: {original_bus_plan_state.total_bus_minutes}")

original_score = all_benchmarks(original_bus_plan_state, times=5)
original_bus_minutes = original_bus_plan_state.total_bus_minutes

improved_bps = copy.deepcopy(original_bus_plan_state)
improved_bps.name = "improved"
improved_bps.save_folder = Path(".")





def increase_random_route_frequency(bps, by_minutes: int = 5):
    route_options = list(bps.routes_to_stops.keys())
    random_route = random.choice(route_options)
    did_increase_freq = False
    if bps.shortest_intervals[random_route] > by_minutes:
        bps.shortest_intervals[random_route] -= by_minutes
        did_increase_freq = True
    
    description = f"On route {random_route} increased frequency by {by_minutes}"
    
    def undo(bps):
        if did_increase_freq:
            bps.shortest_intervals[random_route] += by_minutes

    return description, undo


def decrease_random_route_frequency(bps, by_minutes: int = 5):
    route_options = list(bps.routes_to_stops.keys())
    random_route = random.choice(route_options)
    bps.shortest_intervals[random_route] += by_minutes

    description = f"On route {random_route} decreased frequency by {by_minutes}"
    
    def undo(bps):
         bps.shortest_intervals[random_route] -= by_minutes

    return description, undo


def replace_random_stop(bps, radius_meters: int = 2400):
    route_options = list(bps.routes_to_stops.keys())
    random_route = random.choice(route_options)
    random_idx = random.choice(range(len(bps.routes_to_stops[random_route])))
    
    old_stop = improved_bps.routes_to_stops[random_route][random_idx]
    nearby_stops = bps.node_attributes[util.find_all_within(bps.node_attributes.geometry.loc[old_stop], bps.node_attributes.geometry, radius_meters, bps.cosine_latitude)].index
    random_new_stop = random.choice(nearby_stops)

    bps.replace_ith_stop_on_route(random_idx, random_route, random_new_stop)
    description = f"On route {random_route}, replaced stop {old_stop} {dataset.node_attributes.loc[old_stop].stop_name} with {random_new_stop} {dataset.node_attributes.loc[random_new_stop].stop_name} in radius {radius_meters}"
    
    def undo(bps):
        bps.replace_ith_stop_on_route(random_idx, random_route, old_stop)

    return description, undo

def add_random_stop(bps, radius_meters: int = 2400):
    route_options = list(bps.routes_to_stops.keys())
    random_route = random.choice(route_options)
    random_idx = random.choice(range(len(bps.routes_to_stops[random_route])))
    
    current_stop = improved_bps.routes_to_stops[random_route][random_idx]
    nearby_stops = bps.node_attributes[util.find_all_within(bps.node_attributes.geometry.loc[current_stop], bps.node_attributes.geometry, 2400, bps.cosine_latitude)].index
    random_new_stop = random.choice(nearby_stops)

    bps.insert_ith_stop_on_route(random_idx, random_route, random_new_stop)
    description = f"On route {random_route}, added stop {random_new_stop} {dataset.node_attributes.loc[random_new_stop].stop_name} in radius {radius_meters}"
    
    def undo(bps):
        bps.remove_ith_stop_on_route(random_idx, random_route)
    return description, undo

 
def remove_random_stop(bps, radius_meters: int = 0):
    route_options = list(bps.routes_to_stops.keys())
    random_route = random.choice(route_options)
    random_idx = random.choice(range(len(bps.routes_to_stops[random_route])))

    old_stop = improved_bps.routes_to_stops[random_route][random_idx]
    bps.remove_ith_stop_on_route(random_idx, random_route)

    description = f"On route {random_route}, removed stop {old_stop} {dataset.node_attributes.loc[old_stop].stop_name}"
    
    def undo(bps):
        bps.insert_ith_stop_on_route(random_idx, random_route, old_stop)
    return description, undo

increased_bus_mins_actions = [increase_random_route_frequency, add_random_stop]
decreased_bus_mins_actions = [decrease_random_route_frequency, remove_random_stop]
neutral_actions = [replace_random_stop]

best_score = original_score
logger.info(f"Current best: {best_score}")

Q_params = {
        increase_random_route_frequency.__name__: {5: 0, 10: 0, 20: 0},
        decrease_random_route_frequency.__name__: {5: 0, 10: 0, 20: 0},
        add_random_stop.__name__: {800: 0, 1600: 0, 3200: 0},
        remove_random_stop.__name__: {0: 0},
        replace_random_stop.__name__: {800: 0, 1600: 0, 3200: 0}
}

Q_update_counts = {
        increase_random_route_frequency.__name__: {5: 1, 10: 1, 20: 1},
        decrease_random_route_frequency.__name__: {5: 1, 10: 1, 20: 1},
        add_random_stop.__name__: {800: 1, 1600: 1, 3200: 1},
        remove_random_stop.__name__: {0: 1},
        replace_random_stop.__name__: {800: 1, 1600: 1, 3200: 1}
}

def choose_optimal_action(action_list):
    return min(action_list, key = lambda a: min(Q_params[a.__name__].values()))
def get_optimal_param(action):
    return min(Q_params[action.__name__], key = lambda p: Q_params[action.__name__][p])

INITIAL_EXPLORE_PROBABILITY = 0.7
TOLERANCE_PROBABILITY = 0.2
EPOCHS = 200

scores = []
bus_minutes = []
for i in tqdm(range(EPOCHS)):
    current_bus_minutes = improved_bps.total_bus_minutes
    bus_minutes.append(current_bus_minutes)

    if current_bus_minutes > original_bus_minutes * 1.1:
        possible_actions = decreased_bus_mins_actions + neutral_actions
    else:
        possible_actions = increased_bus_mins_actions + decreased_bus_mins_actions + neutral_actions

    if random.random() < INITIAL_EXPLORE_PROBABILITY * (0.99 ** i):
        action = random.choice(possible_actions)
        params = random.choice(list(Q_params[action.__name__].keys()))
    else:
        action = choose_optimal_action(possible_actions)
        params = get_optimal_param(action)

    description, undo = action(improved_bps, params)

    current_score = all_benchmarks(improved_bps, times=5)
    scores.append(current_score)


    logging.info(str(current_score))
    logging.info(Q_params)

    additional_tolerance = best_score * .1 if random.random() < TOLERANCE_PROBABILITY else 0

    n = Q_update_counts[action.__name__][params] + 1
    Q_params[action.__name__][params] = (1/n) * (current_score - best_score) + (1 - 1/n) *  Q_params[action.__name__][params]
    Q_update_counts[action.__name__][params] = n

    if current_score < best_score + additional_tolerance:
        best_score = current_score
        logger.info(description)
        logger.info(f"Current best: {best_score}")
        
        logger.info(f"improved valid: {improved_bps.valid}")

        logger.info(f"improved total time: {improved_bps.total_bus_minutes}")
        
        improved_bps.save()
    else:
        undo(improved_bps)

logger.info(",".join([str(s) for s in scores]))
logger.info(",".join([str(bm) for bm in bus_minutes]))
plt.plot(scores)
plt.title('Scores')
plt.show()

plt.plot(bus_minutes)
plt.title('Bus Minutes')
plt.show()

logger.info(f"Improvement: {current_score - original_score}")

improved_bps.save()
viz = util.visualize_bps_diff(original_bus_plan_state, improved_bps, dataset.node_attributes)
show(viz)