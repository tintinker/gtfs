import numpy as np
import random
from lib.delay_dataset import DelayDataset
from lib.realtime import RealtimeWatcher
from lib.route_plan_dataset import RoutePlanDataset
from threading import Thread
import os

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

    dataset = RoutePlanDataset.load("datasets/new_orleans")
    dataset2 = DelayDataset.load("datasets/sanfrancisco_delays")

    SF_API_KEY = os.environ["SFKEY"]
    MIAMI_API_KEY = os.environ["MIAMIKEY"]

    sf_watcher = RealtimeWatcher("data/sanfrancisco/sanfrancisco_gtfs.zip", "https://api.511.org/transit/tripupdates?agency=SF", -8, "realtime/sanfrancisco", api_key=SF_API_KEY, resuming_from_previous=False)
    miami_watcher = RealtimeWatcher("data/miami/miami_gtfs.zip", "https://api.goswift.ly/real-time/miami/gtfs-rt-trip-updates", -5, "realtime/miami", api_key=MIAMI_API_KEY, resuming_from_previous=False)
    philedelphia_watcher = RealtimeWatcher("data/philadelphia/philadelphia_gtfs.zip", "https://www3.septa.org/gtfsrt/septa-pa-us/Trip/rtTripUpdates.pb", -5, "realtime/philadelphia", resuming_from_previous=False)

    threads = [sf_watcher.watch(), miami_watcher.watch(), philedelphia_watcher.watch()]
    for t in threads:
        t.join()

    




    
    



    
    