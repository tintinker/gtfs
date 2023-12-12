import random
import lib.util as util

def increase_route_frequency(bps, route_id, by_minutes):
    did_increase_freq = False
    if bps.shortest_intervals[route_id] > by_minutes:
        bps.shortest_intervals[route_id] -= by_minutes
        did_increase_freq = True
    
    description = f"On route {route_id} increased frequency by {by_minutes}"
    
    def undo(bps):
        if did_increase_freq:
            bps.shortest_intervals[route_id] += by_minutes

    return description, undo


def decrease_route_frequency(bps, route_id, by_minutes):
    bps.shortest_intervals[route_id] += by_minutes

    description = f"On route {route_id} decreased frequency by {by_minutes}"
    
    def undo(bps):
         bps.shortest_intervals[route_id] -= by_minutes

    return description, undo


def replace_stop(bps, route_id, stop_idx, new_stop):    
    old_stop = bps.routes_to_stops[route_id][stop_idx]
    nearby_stops = bps.node_attributes[util.find_all_within(bps.node_attributes.geometry.loc[old_stop], bps.node_attributes.geometry, radius_meters, bps.cosine_latitude)].index

    bps.replace_ith_stop_on_route(stop_idx, route_id, new_stop)
    description = f"On route {route_id}, replaced stop {old_stop} {bps.node_attributes.loc[old_stop].stop_name} with {new_stop} {bps.node_attributes.loc[new_stop].stop_name}"
    
    def undo(bps):
        bps.replace_ith_stop_on_route(stop_idx, route_id, old_stop)

    return description, undo

def add_stop(bps, route_id, stop_idx, new_stop):    
    current_stop = bps.routes_to_stops[route_id][stop_idx]
    nearby_stops = bps.node_attributes[util.find_all_within(bps.node_attributes.geometry.loc[current_stop], bps.node_attributes.geometry, 2400, bps.cosine_latitude)].index

    bps.insert_ith_stop_on_route(stop_idx, route_id, new_stop)
    description = f"On route {route_id}, added stop {new_stop} {bps.node_attributes.loc[new_stop].stop_name}"
    
    def undo(bps):
        bps.remove_ith_stop_on_route(stop_idx, route_id)
    return description, undo

 
def remove_stop(bps, route_id, stop_idx):
    old_stop = bps.routes_to_stops[route_id][stop_idx]
    bps.remove_ith_stop_on_route(stop_idx, route_id)

    description = f"On route {route_id}, removed stop {old_stop} {bps.node_attributes.loc[old_stop].stop_name}"
    
    def undo(bps):
        bps.insert_ith_stop_on_route(stop_idx, route_id, old_stop)
    return description, undo

increased_bus_mins_actions = [increase_route_frequency, add_stop]
decreased_bus_mins_actions = [decrease_route_frequency, remove_stop]
neutral_actions = [replace_stop]