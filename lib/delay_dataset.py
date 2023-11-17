from lib.dataset import Dataset
import random
import os
import util
from torch_geometric.utils import from_networkx
import networkx as nx
import contextily as ctx
import matplotlib.pyplot as plt

class DelayDataset(Dataset):
    @staticmethod
    def load(folder):
        dataset = Dataset.load(folder)
        dataset.__class__ = DelayDataset
        return dataset
    
    def pyg_data(self, node_attribute_names, edge_attribute_names):
        graph_with_attrs = self.G.copy()
        nx.set_node_attributes(graph_with_attrs, self.node_attributes[node_attribute_names].to_dict(orient='index'))
        nx.set_edge_attributes(graph_with_attrs, self.edge_attriutes[edge_attribute_names].to_dict(orient='index'))
        return from_networkx(graph_with_attrs, node_attribute_names, edge_attribute_names)


    def visualize(self, shp_folder = None, num_route_samples = 20):
        all_unique_routes = list(self.feed.routes.route_id.unique())
        routes = random.sample(all_unique_routes, min(num_route_samples, len(all_unique_routes)))
        
        view, ok = util.filter_graph(
            self.G, 
            filter_edge = lambda g, u_node, v_node: set(self.edge_attriutes.loc[u_node, v_node]["routes"]).intersection(routes), 
            filter_node = lambda g, node: g.degree[node] > 0
            )

        routes_viz = util.visualize_routes(view, self.node_attributes, self.edge_attriutes)

        if shp_folder:
            routes_viz.to_file(os.path.join(shp_folder, "routes_viz.shp"))

        
        return routes_viz
    
    
    
if __name__ == "__main__":
    # dataset = DelayDataset("sanfrancisco", "data/sanfrancisco/sanfrancisco_gtfs.zip", save_folder="datasets/sanfrancisco", include_delay=True, delay_sqlite_db_str="data/sanfrancisco/sanfrancisco.db")
    # dataset.build()

    dataset = DelayDataset("la", "data/la/la_gtfs.zip", save_folder="datasets/la", include_delay=True, delay_sqlite_db_str="data/la/la.db")
    dataset.build()

    dataset = DelayDataset("miami", "data/miami/miami_gtfs.zip", save_folder="datasets/miami", include_delay=True, delay_sqlite_db_str="data/miami/miami.db")
    dataset.build()

    # dataset = DelayDataset.load("data/sanfrancisco/dataset")
    # gdf_edges = dataset.to_shapefile(num_route_samples=50)
    # print(dataset.pyg_data(node_attribute_names=['stop_lat', 'stop_lon'], edge_attribute_names=['driving_time', 'avg_delay']))
    
    # fig, ax = plt.subplots(figsize=(10, 8))
    # gdf_edges.plot(ax=ax, color=gdf_edges.color)
    # ctx.add_basemap(ax, crs='EPSG:4326') 
    # plt.show()