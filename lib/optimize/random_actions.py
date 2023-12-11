import random
import lib.util as util

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
    
    old_stop = bps.routes_to_stops[random_route][random_idx]
    nearby_stops = bps.node_attributes[util.find_all_within(bps.node_attributes.geometry.loc[old_stop], bps.node_attributes.geometry, radius_meters, bps.cosine_latitude)].index
    random_new_stop = random.choice(nearby_stops)

    bps.replace_ith_stop_on_route(random_idx, random_route, random_new_stop)
    description = f"On route {random_route}, replaced stop {old_stop} {bps.node_attributes.loc[old_stop].stop_name} with {random_new_stop} {bps.node_attributes.loc[random_new_stop].stop_name} in radius {radius_meters}"
    
    def undo(bps):
        bps.replace_ith_stop_on_route(random_idx, random_route, old_stop)

    return description, undo

def add_random_stop(bps, radius_meters: int = 2400):
    route_options = list(bps.routes_to_stops.keys())
    random_route = random.choice(route_options)
    random_idx = random.choice(range(len(bps.routes_to_stops[random_route])))
    
    current_stop = bps.routes_to_stops[random_route][random_idx]
    nearby_stops = bps.node_attributes[util.find_all_within(bps.node_attributes.geometry.loc[current_stop], bps.node_attributes.geometry, 2400, bps.cosine_latitude)].index
    random_new_stop = random.choice(nearby_stops)

    bps.insert_ith_stop_on_route(random_idx, random_route, random_new_stop)
    description = f"On route {random_route}, added stop {random_new_stop} {bps.node_attributes.loc[random_new_stop].stop_name} in radius {radius_meters}"
    
    def undo(bps):
        bps.remove_ith_stop_on_route(random_idx, random_route)
    return description, undo

 
def remove_random_stop(bps, radius_meters: int = 0):
    route_options = list(bps.routes_to_stops.keys())
    random_route = random.choice(route_options)
    random_idx = random.choice(range(len(bps.routes_to_stops[random_route])))

    old_stop = bps.routes_to_stops[random_route][random_idx]
    bps.remove_ith_stop_on_route(random_idx, random_route)

    description = f"On route {random_route}, removed stop {old_stop} {bps.node_attributes.loc[old_stop].stop_name}"
    
    def undo(bps):
        bps.insert_ith_stop_on_route(random_idx, random_route, old_stop)
    return description, undo

increased_bus_mins_actions = [increase_random_route_frequency, add_random_stop]
decreased_bus_mins_actions = [decrease_random_route_frequency, remove_random_stop]
neutral_actions = [replace_random_stop]


