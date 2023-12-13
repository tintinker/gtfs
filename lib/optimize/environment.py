import torch
import fixed_actions as actions
import numpy as np
import lib.util as util

#action spec = [type route_representation old_stop_pref_representation new_stop_pref_representation]
#type = [one hot encoding: increase_freq, decrease_freq, add_stop, remove_stop, replace_stop]
#route_representation = [num_stops, total_driving_time, freq, lat_mean, lat_std, lng_mean, lng_std]
#old_stop_pref_representation = [old_stop_near_lat, old_stop_near_lng]
#new_stop_pref_representation = [distance_from_old_stop]

ACTION_TYPES = [
    actions.increase_route_frequency,
    actions.decrease_route_frequency, 
    actions.add_stop, 
    actions.remove_stop,
    actions.replace_stop
]

def valid_action_tensor(action_tensor):
    return action_tensor.shape[1] == len(ACTION_TYPES)

def parse_action_tensor(action_tensor):
    action = None
    for i in range(5):
        if action_tensor[i] == 1:
            action = ACTION_TYPES[i]
            break
    route_pref = action_tensor[5:12]
    old_stop_pref = action_tensor[12:14]
    new_stop_pref = action_tensor[14]
    return action, route_pref, old_stop_pref, new_stop_pref

def preferred_route(self, route_pref):
    best_route = np.argmin(np.linalg.norm(self.route_matrix - route_pref))
    return self.route_ids[best_route]

def preferred_old_stop_idx(self, route, old_stop_pref):
    best_idx = np.argmin(np.linalg.norm(self.stop_matrices[route] - old_stop_pref))
    return best_idx

def choose_new_stop(self, route, old_stop_idx, new_stop_pref):
    radius_meters = new_stop_pref
    nearby_stops = self.bps.node_attributes[util.find_all_within(self.bps.node_attributes.geometry.loc[old_stop_idx], self.bps.node_attributes.geometry, radius_meters, self.bps.cosine_latitude)].index
    

class SimpleEnvironment:
    def __init__(self, num_states):
        self.num_states = num_states
        self.current_state = 0

    def step(self, action):
        # Assuming a cyclic transition through states every 10 episodes
        self.current_state = (self.current_state + 1) % self.num_states

        # For simplicity, assume a reward of 1 when reaching the last state
        reward = 1.0 if self.current_state == self.num_states - 1 else 0.0

        # For simplicity, assume the episode ends after reaching the last state
        done = (self.current_state == self.num_states - 1)

        return torch.FloatTensor([self.current_state]), reward, done