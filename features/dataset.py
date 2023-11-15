from census import CensusData
from osm import OpenStreetMapsData
from shapely.geometry import Point
import numpy as np
import geopandas as gpd
import json
from gtfs_functions import Feed
from tqdm import tqdm
import pandas as pd
from shapely import wkt
from ast import literal_eval

tqdm.pandas()

METERS_TO_DEGREE = 111111 #https://gis.stackexchange.com/questions/2951/algorithm-for-offsetting-a-latitude-longitude-by-some-amount-of-meters
NEARBY_STOP_THRESHOLD = 200
NEARBY_POI_THRESHOLD = 400
#osm = OpenStreetMapsData(37.71496628274509,  -122.54870245854397, 37.80752063920071, -122.35781501086306)


def find_all_within(origin: Point, options: gpd.GeoSeries, distance_in_meters):
    distances = options.apply(lambda d: approx_distance_in_meters(origin, d))
    mask = distances <= distance_in_meters
    return mask

def find_closest(origin: Point, options: gpd.GeoSeries):
    distances = options.apply(lambda d: approx_distance_in_meters(origin, d))
    idx = np.argmin(distances)
    return idx, distances[idx]

def approx_distance_in_meters(origin: Point, destination: Point):
    x_dist = np.cos(origin.x) * METERS_TO_DEGREE * np.abs(origin.x  - destination.x)
    y_dist =  METERS_TO_DEGREE * np.abs(origin.y  - destination.y)
    return np.sqrt(x_dist ** 2 + y_dist ** 2)




class Dataset:
    def __init__(self, name, gtfs_zip_filename, nearby_stop_threshold = 200, nearby_poi_threshold = 400, census_tables_and_groupings = ("features/census_tables.yaml", "features/census_groupings.yaml"), ):
        self.gtfs_source = gtfs_zip_filename
        self.feed = Feed(gtfs_zip_filename)
        self.stops_data = gpd.GeoDataFrame(self.feed.stops.copy(), geometry="geometry")
        self.osm = OpenStreetMapsData(self.stops_data.stop_lat.min(), self.stops_data.stop_lon.min(), self.stops_data.stop_lat.max(), self.stops_data.stop_lon.max())
        census_tables, census_groupings = census_tables_and_groupings
        self.census = CensusData(census_tables, census_groupings, geo_cache=f"{name}_census_geo.cache", data_cache=f"{name}_census_data.cache" )
        self.min_distance_fields = []
        self.collapsed_stop_mapping = {}
        self.nearby_stop_threshold = nearby_stop_threshold
        self.nearby_poi_threshold = nearby_poi_threshold

    def _download_osm_data(self):
        self.min_distance_fields = []
        hospital_info = ("hospital", self.osm.find_hospitals())
        grocery_info = ("grocery", self.osm.find_grocery_store())
        parks_info = ("park", self.osm.find_parks())
        bars_info = ("bar", self.osm.find_bars())
        worship_info = ("worship", self.osm.find_worship())
        mcdonalds_info = ("mcdonalds", self.osm.find_mcdonalds())
        starbucks_info = ("starbucks", self.osm.find_starbucks())

        for poi_name, poi_gdf in tqdm([hospital_info, grocery_info, parks_info, bars_info, worship_info, mcdonalds_info, starbucks_info]):
            tqdm.write(poi_name)
            self.stops_data[f"closest_{poi_name}_distance"] = self.stops_data.geometry.apply(lambda p: find_closest(p, poi_gdf.geometry)[1])
            self.min_distance_fields.append(f"closest_{poi_name}_distance")

    def _collapse_stops(self):
        self.collapsed_stop_mapping = {}
        self.stops_data["nearby_stops"] = self.stops_data.progress_apply(lambda row: self.stops_data[find_all_within(row["geometry"], self.stops_data.geometry, self.nearby_stop_threshold)].stop_id.tolist(), axis=1)
        
        i = 0
        with tqdm(total=len( self.stops_data)) as pbar:
            while i < len( self.stops_data):

                mask = self.stops_data.stop_id.apply(lambda s: int(s) not in self.collapsed_stop_mapping.values() and int(s) in map(int, self.stops_data.nearby_stops.iloc[i]))
                data = self.stops_data[mask]
                self.stops_data = self.stops_data[~mask]
                for mdf in self.min_distance_fields:
                    self.stops_data[mdf].iloc[i] = np.min([self.stops_data[mdf].iloc[i]] + data[mdf].tolist())
                for nb in self.stops_data.nearby_stops.iloc[i]:
                    self.collapsed_stop_mapping[int(nb)] = int(self.stops_data.stop_id.iloc[i])
                
                i += 1
                pbar.n = i
                pbar.total = len(self.stops_data)
                pbar.refresh()
    
    def _add_census_data(self):
        self.census.add_locations_from_geodataframe(self.stops_data)
        census_data = self.census.get_all_location_data(download=True)
        self.stops_data =  self.stops_data.merge(census_data, on="geometry").reset_index()

        self.collapsed_stop_mapping = {k: self.stops_data.index[ self.stops_data.stop_id == v].values[0] for (k,v) in self.collapsed_stop_mapping.items()}
        self.collapsed_stop_mapping.update({v:self.stops_data.index[ self.stops_data.stop_id == v].values[0] for (_,v) in self.collapsed_stop_mapping.items()})






