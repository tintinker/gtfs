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
        good_nodes = self.edge_attributes[self.node_attributes[node_attribute_names].notna().all(axis=1)].index.tolist()
        graph_with_attrs = graph_with_attrs.subgraph(good_nodes)
        graph_with_attrs = graph_with_attrs.edge_subgraph(good_edges)

        nx.set_node_attributes(graph_with_attrs, node_attributes[node_attribute_names].to_dict(orient='index'))
        nx.set_edge_attributes(graph_with_attrs, edge_attributes[edge_attribute_names].to_dict(orient='index'))
       
        
        return from_networkx(graph_with_attrs, node_attribute_names, edge_attribute_names)

    def add_predictions(self, df):
        self.edge_attributes = self.edge_attributes.merge(df, how="left")

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

       
    
    
