import numpy as np
import random
from lib.delay_dataset import DelayDataset
from lib.realtime import RealtimeWatcher
from lib.route_plan_dataset import RoutePlanDataset

if __name__ == "__main__":
    np.random.seed(42)
    random.seed(42)
    #https://colab.research.google.com/drive/10Mnw3CPGYQl5Qs40SkT1cM8KagXb-Lwx#

    # dataset = RoutePlanDataset("cleveland", "data/cleveland/gtfs.zip", save_folder="datasets/cleveland")
    # dataset.build()

    # dataset = DelayDataset("sanfrancisco", "data/sanfrancisco/sanfrancisco_gtfs.zip", save_folder="datasets/sanfrancisco_delays", include_delay=True, delay_sqlite_db_str="data/sanfrancisco/sanfrancisco.db")
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

    realtime_watcher = RealtimeWatcher("data/cleveland/gtfs.zip", "", -5, "realtime/cleveland", "", resume_from_previous=False)
    realtime_watcher.watch()




    
    



    
    