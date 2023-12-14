from pathlib import Path
from typing import Union
from lib.dataset import Dataset
import random
import os
import lib.util as util
from torch_geometric.utils import from_networkx
import networkx as nx
import numpy as np
from shapely import Point, LineString
import geopandas as gpd
import torch

class DelayDataset(Dataset):
    @staticmethod
    def load(folder):
        dataset = Dataset.load(folder)
        dataset.__class__ = DelayDataset
        return dataset
    
    def build(self, override_if_already_built = False, use_cache = True, save_folder: Union[str, Path] = None):
        super()._build(override_if_already_built, use_cache, save_folder)
        self.built = True
    
    def pyg_data(self, node_attribute_names, edge_attribute_names):
        graph_with_attrs = self.G.copy()

        node_attributes = self.node_attributes.dropna(subset=node_attribute_names)
        edge_attributes = self.edge_attributes.dropna(subset=edge_attribute_names)

        good_edges = self.edge_attributes[self.edge_attributes[edge_attribute_names].notna().all(axis=1)].index.tolist()
        good_nodes = self.node_attributes[self.node_attributes[node_attribute_names].notna().all(axis=1)].index.tolist()
        graph_with_attrs = graph_with_attrs.subgraph(good_nodes)
        graph_with_attrs = graph_with_attrs.edge_subgraph(good_edges)

        nx.set_node_attributes(graph_with_attrs, node_attributes[node_attribute_names].to_dict(orient='index'))
        nx.set_edge_attributes(graph_with_attrs, edge_attributes[edge_attribute_names].to_dict(orient='index'))
       
        
        return from_networkx(graph_with_attrs, node_attribute_names, edge_attribute_names)

    def visualize_from_pyg(self, data, test_indices, lat_feature_idx, lng_feature_idx, target, predictions):
   
        source_nodes = data.edge_index[0, test_indices]
        dest_nodes = data.edge_index[1, test_indices]
        
        source_lat = data.x[source_nodes, lat_feature_idx]
        source_lng = data.x[source_nodes, lng_feature_idx]
        dest_lat = data.x[dest_nodes, lat_feature_idx]
        dest_lng = data.x[dest_nodes, lng_feature_idx]

        target = target[test_indices]
        predictions = predictions[test_indices]

        everything = torch.concat((
            source_nodes.unsqueeze(1),
            source_lat.unsqueeze(1),
            source_lng.unsqueeze(1),
            dest_nodes.unsqueeze(1),
            dest_lat.unsqueeze(1),
            dest_lng.unsqueeze(1),
            target.unsqueeze(1),
            predictions.unsqueeze(1)
        ), dim=1)
        
        target_data = []
        prediction_data = []
        for i in range(everything.shape[0]):
            line = LineString([
                (everything[i, 1], everything[i, 2]),
                (everything[i, 4], everything[i, 5])
                ])
            t = everything[i, -2]
            p = everything[i, -1]

            target_data.append({
                'from': everything[i, 0],
                    'to':  everything[i, 3],
                    'target': t,
                    'geometry': line,
                    'color_map_field': t,
                })
            prediction_data.append({
                'from': everything[i, 0],
                    'to':  everything[i, 3],
                    'target': p,
                    'geometry': line,
                    'color_map_field': p,
                })
        target_gdf = gpd.GeoDataFrame(target_data, crs="EPSG:4326")
        prediction_gdf = gpd.GeoDataFrame(prediction_data, crs="EPSG:4326")
        return target_gdf, prediction_gdf



    def visualize(self, feature="avg_delay", shp_folder = None):        
        good_edges = self.edge_attributes[self.edge_attributes[feature].notna()].index.tolist()
        subgraph = self.G.edge_subgraph(good_edges)

        edges_data = []
        for u, v in subgraph.edges:
            line = LineString([
                (self.node_attributes.loc[u]['stop_lon'], self.node_attributes.loc[u]['stop_lat']),
                (self.node_attributes.loc[v]['stop_lon'], self.node_attributes.loc[v]['stop_lat'])
                ])
            values = self.edge_attributes.loc[u,v][feature] if feature in self.edge_attributes else np.nan
            
            edges_data.append({
                'from': self.node_attributes.loc[u]['stop_name'],
                    'to':  self.node_attributes.loc[v]['stop_name'],
                    'routes': self.edge_attributes.loc[u,v]['routes'],
                    feature: values,
                    'geometry': line,
                    'color_map_field': values,
                })
        gdf = gpd.GeoDataFrame(edges_data, crs="EPSG:4326")

        if shp_folder:
            gdf.routes = gdf.routes.apply(lambda r: ','.join(map(str, r)))
            gdf.to_file(os.path.join(shp_folder, "viz.shp"))

        return gdf

       
    
    
