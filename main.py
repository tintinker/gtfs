import numpy as np
import random
from lib.delay_dataset import DelayDataset
from lib.realtime import RealtimeWatcher
from lib.route_plan_dataset import RoutePlanDataset
import contextily as ctx
import matplotlib.pyplot as plt
import lib.util as util

if __name__ == "__main__":
    np.random.seed(42)
    random.seed(42)

    # dataset = DelayDataset.load("datasets/philadelphia_delay")
    # chosen_node_features = ['stop_lat', 'stop_lon', 'near_starbucks', 'total_population','population_over_age_25', 'has_bachelors_degree_percent','income_to_poverty_under_200']
    # chosen_edge_features = ['avg_delay','driving_time']
    # print(dataset.pyg_data(node_attribute_names=chosen_node_features, edge_attribute_names=chosen_edge_features))
    
    # util.show_viz(dataset.visualize(shp_folder="."))
    # dataset = DelayDataset("sanfrancisco_delay_full",  "gtfs_data/2023_december/sanfrancisco_gtfs.zip", "census_boundaries_data/2021/California", save_folder="datasets/sanfrancisco_delay_full", include_delay=True, delay_sqlite_db_str="realtime/sanfrancisco/realtime.db", only_during_peak=False)
    # dataset.build()

    # dataset = DelayDataset("sanfrancisco_delay_peak",  "gtfs_data/2023_december/sanfrancisco_gtfs.zip", "census_boundaries_data/2021/California", save_folder="datasets/sanfrancisco_delay_peak", include_delay=True, delay_sqlite_db_str="realtime/sanfrancisco/realtime.db", only_during_peak=True)
    # dataset.build()

    dataset = DelayDataset("philadelphia_delay_full",  "gtfs_data/2023_december/philadelphia_gtfs.zip", "census_boundaries_data/2021/PennDelawareNJ", save_folder="datasets/philadelphia_delay_full", include_delay=True, delay_sqlite_db_str="realtime/philadelphia/realtime.db", only_during_peak=False)
    dataset.build()

    dataset = DelayDataset("philadelphia_delay_peak",  "gtfs_data/2023_december/philadelphia_gtfs.zip", "census_boundaries_data/2021/PennDelawareNJ", save_folder="datasets/philadelphia_delay_peak", include_delay=True, delay_sqlite_db_str="realtime/philadelphia/realtime.db", only_during_peak=True)
    dataset.build()

    # dataset = DelayDataset("los_angeles_delay_full",  "gtfs_data/2023_december/los_angeles_gtfs.zip", "census_boundaries_data/2021/California", save_folder="datasets/los_angeles_delay_full", include_delay=True, delay_sqlite_db_str="realtime/la/realtime.db", only_during_peak=False)
    # dataset.build()

    # dataset = DelayDataset("los_angeles_delay_peak",  "gtfs_data/2023_december/los_angeles_gtfs.zip", "census_boundaries_data/2021/California", save_folder="datasets/los_angeles_delay_peak", include_delay=True, delay_sqlite_db_str="realtime/la/realtime.db", only_during_peak=True)
    # dataset.build()

    

    # dataset = DelayDataset("miami",  "gtfs_data/2023_december/miami_gtfs.zip", save_folder="datasets/miami_delay", include_delay=True, delay_sqlite_db_str="realtime/miami/realtime.db", census_boundaries_file="census_boundaries_data/2021/Florida")
    # dataset.build()

    # dataset = DelayDataset("los_angeles",  "gtfs_data/2023_december/la_gtfs.zip", save_folder="datasets/los_angeles_delay", include_delay=True, delay_sqlite_db_str="realtime/la/realtime.db", census_boundaries_file="census_boundaries_data/2021/California")
    # dataset.build()

    # dataset = DelayDataset("philadelphia",  "gtfs_data/2023_december/philadelphia_gtfs.zip", save_folder="datasets/philadelphia_delay", include_delay=True, delay_sqlite_db_str="realtime/philadelphia/realtime.db", census_boundaries_file="census_boundaries_data/2021/Pennsylvania")
    # dataset.build()

    # dataset = DelayDataset("cleveland",  "gtfs_data/2023_december/cleveland_gtfs.zip", save_folder="datasets/cleveland_delay", include_delay=True, delay_sqlite_db_str="realtime/cleveland/realtime.db", census_boundaries_file="census_boundaries_data/2021/Ohio")
    # dataset.build()

    # dataset = DelayDataset("new_orleans",  "gtfs_data/2023_december/new_orleans_gtfs.zip", save_folder="datasets/new_orleans_delay", include_delay=True, delay_sqlite_db_str="realtime/new_orleans/realtime.db", census_boundaries_file="census_boundaries_data/2021/Louisiana")
    # dataset.build()

    # dataset = RoutePlanDataset("washington_dc", "data/dc_gtfs.zip", save_folder="datasets/washington_dc")
    # dataset.build()

    # dataset = RoutePlanDataset("toronto", "data/toronto_gtfs.zip", save_folder="datasets/toronto", include_census=False)
    # dataset.build()

    # dataset = RoutePlanDataset("new_orleans", "data/new_orleans_gtfs.zip", save_folder="datasets/new_orleans")
    # dataset.build()

    # dataset = RoutePlanDataset("atlanta", "data/atlanta_gtfs.zip", save_folder="datasets/atlanta")
    # dataset.build()

    # dataset = RoutePlanDataset("vancouver", "data/vancouver_gtfs.zip", save_folder="datasets/vancouver", include_census=False)
    # dataset.build()
    # from secret import SF_API_KEY, MIAMI_API_KEY, LA_API_KEY, NOLA_API_KEY
    
    # sf_watcher = RealtimeWatcher(
    #     "gtfs_data/2023_december/sanfrancisco_gtfs.zip", 
    #     "https://api.511.org/transit/tripupdates?agency=SF", 
    #     -8, "realtime/sanfrancisco", 
    #     api_key=SF_API_KEY, 
    #     resuming_from_previous=True
    #     )
    # miami_watcher = RealtimeWatcher(
    #     "gtfs_data/2023_december/miami_gtfs.zip", 
    #     "https://api.goswift.ly/real-time/miami/gtfs-rt-trip-updates", 
    #     -5, "realtime/miami", 
    #     api_key=MIAMI_API_KEY, 
    #     resuming_from_previous=True
    # )
    # la_watcher = RealtimeWatcher(
    #     "gtfs_data/2023_december/la_gtfs.zip", 
    #     "https://api.goswift.ly/real-time/lametro/gtfs-rt-trip-updates", 
    #     -8, "realtime/la", 
    #     api_key=LA_API_KEY, 
    #     resuming_from_previous=True
    # )
    # philedelphia_watcher = RealtimeWatcher(
    #     "gtfs_data/2023_december/philadelphia_gtfs.zip", 
    #     "https://www3.septa.org/gtfsrt/septa-pa-us/Trip/rtTripUpdates.pb", 
    #     -5, "realtime/philadelphia", 
    #     resuming_from_previous=True
    # )
    # cleveland_watcher = RealtimeWatcher(
    #     "gtfs_data/2023_december/cleveland_gtfs.zip", 
    #     "https://gtfs-rt.gcrta.vontascloud.com/TMGTFSRealTimeWebService/TripUpdate/TripUpdates.pb", 
    #     -5, "realtime/cleveland", 
    #     resuming_from_previous=True
    # )
    # nola_watcher = RealtimeWatcher(
    #     "gtfs_data/2023_december/new_orleans_gtfs.zip", 
    #     "https://bustime.norta.com/gtfsrt/trips", 
    #     -6, "realtime/new_orleans", 
    #     api_key=NOLA_API_KEY, 
    #     resuming_from_previous=True
    # )

    # threads = [
    #     sf_watcher.watch(),
    #     miami_watcher.watch(), 
    #     la_watcher.watch(),
    #     philedelphia_watcher.watch(),
    #     nola_watcher.watch(),
    #     cleveland_watcher.watch()
    # ]
    # for t in threads:
    #     t.join()

    




    
    



    
    