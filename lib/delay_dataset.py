from pathlib import Path
from typing import Union
from lib.dataset import Dataset
import random
import os
import lib.util as util
from torch_geometric.utils import from_networkx
import networkx as nx

class DelayDataset(Dataset):
    @staticmethod
    def load(folder):
        dataset = Dataset.load(folder)
        dataset.__class__ = DelayDataset
        return dataset
    
    def build(self, override_if_already_built = False, use_cache = True, save_folder: Union[str, Path] = None):
        super()._build(override_if_already_built, use_cache, save_folder)
        self.built = True
    
    def pyg_data(self, node_attribute_names, edge_attribute_names, exclude_edges = None):
        graph_with_attrs = self.G.copy()
        
        node_attributes = self.node_attributes.dropna(subset=node_attribute_names)
        edge_attributes = self.edge_attributes.dropna(subset=edge_attribute_names)

        nx.set_node_attributes(graph_with_attrs, node_attributes[node_attribute_names].to_dict(orient='index'))
        nx.set_edge_attributes(graph_with_attrs, edge_attributes[edge_attribute_names].to_dict(orient='index'))
        if exclude_edges:
            graph_with_attrs.remove_edges_from(exclude_edges)
        
        return from_networkx(graph_with_attrs, node_attribute_names, edge_attribute_names)


    def visualize(self, shp_folder = None, num_route_samples = 20):
        all_unique_routes = list(self.routes.route_id.unique())
        routes = random.sample(all_unique_routes, min(num_route_samples, len(all_unique_routes)))
        
        view, ok = util.filter_graph(
            self.G, 
            filter_edge = lambda g, u_node, v_node: set(self.edge_attributes.loc[u_node, v_node]["routes"]).intersection(routes), 
            filter_node = lambda g, node: g.degree[node] > 0
            )

        routes_viz = util.visualize_delay(view, self.node_attributes, self.edge_attributes)

        if shp_folder:
            routes_viz.routes = routes_viz.routes.apply(lambda r: ','.join(map(str, r)))
            routes_viz.to_file(os.path.join(shp_folder, "routes_viz.shp"))

        
        return routes_viz
    
    
