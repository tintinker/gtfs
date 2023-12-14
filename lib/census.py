from collections import defaultdict
import pandas as pd
from tqdm import tqdm
import yaml
import requests
from shapely.geometry import Point
import geopandas as gpd
import utm

pd.options.mode.chained_assignment = None 



class Table:
    def __init__(self, name, table_id, row, total_field):
        self.name = name
        self.table_id = table_id
        self.row = row
        self.total_field = total_field

    def __str__(self) -> str:
        return f"{self.table_id}_{str(self.row).zfill(3)}E"

class TableGrouping:
    def __init__(self, groupings, delete_tables=True):
        self.groupings = groupings
        self.delete_tables = delete_tables
        
        self.tables = []
        for group in groupings:
            self.tables.extend(groupings[group]['fields'])

    def apply(self, df):
        for group in self.groupings:
            df[group] = df[self.groupings[group]['fields']].sum(axis=1)
            df[f"{group}_percent"] = df[group] / df[self.groupings[group]['percentage_of']]

        if self.delete_tables:
            df = df.drop(self.tables, axis=1)
            df = df.drop([f"{t}_percent" for t in self.tables], axis=1)

        return df

class Query:
    GEO_FIELDS = ["state","county","tract","block group"]

    def __init__(self, census_data_source, state_code, county_code, logger=None):
        self.census_data_source = census_data_source
        self.state_code = state_code
        self.county_code = county_code
        self.logger = logger

        self.tracts = []
        self.tables = []
        self.groupings = []
        



    def add_groupings_from_yaml(self, yaml_filename):
        with open(yaml_filename, "r") as stream:
            groupings = yaml.safe_load(stream)
        self.groupings += [TableGrouping(groupings)]

    def add_tables_from_yaml(self, yaml_filename):
        with open(yaml_filename, "r") as stream:
            tables = yaml.safe_load(stream)
        self.tables += [Table(t["name"], t["table_id"], t["row"], t["percentage_of"]) for t in tables]

    def clear_tables(self):
        self.tables = []

    def add_tract(self, six_digit_tract):
        self.tracts += [six_digit_tract]

    def __str__(self):
        tracts = list(set(self.tracts))
        return f"https://api.census.gov/data/{self.census_data_source}?get={','.join([str(t) for t in self.tables])}&for=block%20group:*&in=state:{str(self.state_code).zfill(2)}&in=county:{str(self.county_code)}&in=tract:{','.join([str(t) for t in tracts])}"
    
    def get_batched_queries(self):
        tracts = list(set(self.tracts))
        for i in range(0, len(tracts), 30):
            yield [f"https://api.census.gov/data/{self.census_data_source}?get={','.join([str(t) for t in self.tables[j:j+30]])}&for=block%20group:*&in=state:{str(self.state_code).zfill(2)}&in=county:{str(self.county_code)}&in=tract:{','.join([str(t) for t in tracts[i:i+30]])}" for j in range(0, len(self.tables), 30)]

    def get(self):
        dfs = []
        
        all_batched_queries = list(self.get_batched_queries())
        for i, url_list in tqdm(enumerate(all_batched_queries), total=len(all_batched_queries), leave=False):
            df = pd.DataFrame(columns=Query.GEO_FIELDS)
            for url in url_list:
                r = requests.get(url)
                batchdf = pd.DataFrame(r.json())
                batchdf.columns = batchdf.iloc[0]
                batchdf = batchdf.drop(0)
                df = df.merge(batchdf, on=Query.GEO_FIELDS, how='outer')
            dfs.append(df)

        
        df = pd.concat(dfs).apply(pd.to_numeric)

        for t in self.tables:
            df = df.rename(columns={str(t): t.name})
                
        for t in self.tables:
            df[f"{t.name}_percent"] = df[t.name] / df[f"{t.total_field}"]
        
        for g in self.groupings:
            df = g.apply(df)

        return df
    
    def _debug(self, message):
        if self.logger:
            self.logger.debug(message)


class CensusData:
    DATA_SOURCE = "2021/acs/acs5"
    BLOCK_GROUP_YEAR = "2021"

    def __init__(self, census_boundaries_file, tables_file, groupings_file = None, logger=None):
        self.tables_file = tables_file
        self.groupings_file = groupings_file

        self.tract_list = defaultdict(list)
        self.location_list = {}
        self.data = pd.DataFrame(columns=Query.GEO_FIELDS)

        self.census_boundaries_gdf = gpd.read_file(census_boundaries_file)
        self.census_boundaries_spatial_index = self.census_boundaries_gdf.sindex

        self.logger = logger

    def location_request(self, point):
        possible_matches_index = list(self.census_boundaries_spatial_index.intersection(point.bounds))
        possible_matches = self.census_boundaries_gdf.iloc[possible_matches_index]
        precise_matches = possible_matches[possible_matches.intersects(point)]
        
        if precise_matches.empty:
            return None
        
        median_lat = precise_matches.geometry.centroid.y.median()
        median_lon = precise_matches.geometry.centroid.x.median()
        _, _, utm_zone, _ = utm.from_latlon(median_lat, median_lon)
        print(f'EPSG:{32600 + utm_zone}')
        precise_matches["block_group_area"] = precise_matches.to_crs(f'EPSG:{32600 + utm_zone}' ).area
        
        match = precise_matches.iloc[0]

        state_code = match["STATEFP"]
        county_code = match["COUNTYFP"]
        tract_code = match["TRACTCE"]
        block_code = match["BLKGRPCE"]
        bg_area = match["block_group_area"]
        return state_code, county_code, tract_code, block_code, bg_area
        
    
    def add_location(self, point: Point):
        location_data = self.location_request(point)
        self._debug(point)
        state_code, county_code, tract_code, block_code, area = location_data

        longitude, latitude = point.x, point.y
        self.tract_list[(state_code, county_code)].append(tract_code)
        self.location_list[(latitude, longitude)] = (state_code, county_code, tract_code, block_code[-1], area)

    def add_locations_from_geodataframe(self, gdf):
        for _, point in tqdm(gdf.geometry.items(), total=len(gdf.geometry)):
            if (point.y, point.x) not in self.location_list:
                self.add_location(point)

    def lookup_location(self, point: Point):
        longitude, latitude = point.x, point.y
        if (latitude, longitude) not in self.location_list:
            self._debug(f"Location {(latitude, longitude)} not found, adding this location.")
            self.add_location(latitude, longitude)
            self.download_data()

        state_code, county_code, tract_code, block_code, bg_area =  self.location_list[(latitude, longitude)]
        df = self.data[(self.data.state == int(state_code)) & (self.data.county == int(county_code)) & (self.data.tract == int(tract_code)) & (self.data['block group'] == int(block_code))]
        return df.iloc[0].to_dict()
    
    def get_all_location_data(self, download=False):
        if download:
            self.download_data()

        dfs = []
        for (latitude, longitude) in self.location_list:
            state_code, county_code, tract_code, block_code, bg_area =  self.location_list[(latitude, longitude)]
            df = self.data[(self.data.state == int(state_code)) & (self.data.county == int(county_code)) & (self.data.tract == int(tract_code)) & (self.data['block group'] == int(block_code))]
            df["block_group_area"] = bg_area
            if 'geometry' not in df.columns:
                df['geometry'] = Point(longitude, latitude)
            dfs.append(df)
        
        return gpd.GeoDataFrame(pd.concat(dfs))
         
    
    def download_data(self):
        dfs = []
        for (state_code, county_code) in tqdm(self.tract_list):
            query = Query(CensusData.DATA_SOURCE, state_code, county_code, self.logger)
            query.add_tables_from_yaml(self.tables_file)
            if self.groupings_file:
                query.add_groupings_from_yaml(self.groupings_file)
            for tract in self.tract_list[(state_code, county_code)]:
                query.add_tract(tract)

            dfs.append(query.get())

        self.data = pd.concat(dfs)
        
    def _debug(self, message):
        if self.logger:
            self.logger.debug(message)


