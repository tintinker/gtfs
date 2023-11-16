from dataset import Dataset
import random
import geopandas as gpd
import os
from shapely.geometry import LineString
import util
from torch_geometric.utils import from_networkx
import networkx as nx

class DelayDataset(Dataset):
    @staticmethod
    def load(folder):
        dataset = Dataset.load(folder)
        dataset.__class__ = DelayDataset
        return dataset
    
    @property
    def all_node_attribute_names(self):
        return list(self.node_attributes.columns)
    
    @property
    def all_edge_attribute_names(self):
        return list(self.edge_attriutes.columns)
    
    def pyg_data(self, node_attribute_names, edge_attribute_names):
        graph_with_attrs = self.G.copy()
        nx.set_node_attributes(graph_with_attrs, self.node_attributes[node_attribute_names].to_dict(orient='index'))
        nx.set_edge_attributes(graph_with_attrs, self.edge_attriutes[edge_attribute_names].to_dict(orient='index'))
        return from_networkx(graph_with_attrs, node_attribute_names, edge_attribute_names)



    def to_shapefile(self, shp_folder, num_route_samples = 20):
        all_unique_routes = list(self.feed.routes.route_id.unique())
        routes = random.sample(all_unique_routes, min(num_route_samples, len(all_unique_routes)))
        
        view, ok = util.filter_graph(
            self.G, 
            filter_edge = lambda g, u_node, v_node: set(self.edge_attriutes.loc[u_node, v_node]["routes"]).intersection(routes), 
            filter_node = lambda g, node: g.degree[node] > 0
            )

        edges_data = []
        for u, v in view.edges:
            line = LineString([
                (self.node_attributes.loc[u]['stop_lon'], self.node_attributes.loc[u]['stop_lat']),
                (self.node_attributes.loc[v]['stop_lon'], self.node_attributes.loc[v]['stop_lat'])
                ])
            edges_data.append({
                'from': self.node_attributes.loc[u]['stop_name'],
                  'to':  self.node_attributes.loc[v]['stop_name'],
                  'routes': self.edge_attriutes.loc[u,v]['routes'],
                  'avg_delay': self.edge_attriutes.loc[u,v]['avg_delay'],
                  'geometry': line
                  })

        gdf_edges = gpd.GeoDataFrame(edges_data, crs="EPSG:4326")

        gdf_edges.to_file(os.path.join(shp_folder, "edges.shp"))
    
    
    
if __name__ == "__main__":
    # dataset = DelayDataset("sanfrancisco", "data/sanfrancisco/sanfrancisco_gtfs.zip", save_folder="data/sanfrancisco/dataset", include_delay=True, delay_sqlite_db_str="data/sanfrancisco/sanfrancisco.db")
    # dataset.build()
    dataset = DelayDataset.load("data/sanfrancisco/dataset")
    dataset.to_shapefile("shp", 50)
    print(dataset.pyg_data(node_attribute_names=['stop_lat', 'stop_lon'], edge_attribute_names=['driving_time', 'avg_delay']))
