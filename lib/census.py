from collections import defaultdict
import pandas as pd
from tqdm import tqdm
import yaml
import requests
from shapely.geometry import Point
import geopandas as gpd
from pathlib import Path
import json

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

    def __init__(self, census_data_source, state_code, county_code):
        self.census_data_source = census_data_source
        self.state_code = state_code
        self.county_code = county_code

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
        return f"https://api.census.gov/data/{self.census_data_source}?get={','.join([str(t) for t in self.tables])}&for=block%20group:*&in=state:{str(self.state_code).zfill(2)}&in=county:{str(self.county_code)}&in=tract:{','.join([str(t) for t in self.tracts])}"
    
    def get_batched_queries(self):
        for i in range(0, len(self.tracts), 30):
            yield [f"https://api.census.gov/data/{self.census_data_source}?get={','.join([str(t) for t in self.tables[j:j+30]])}&for=block%20group:*&in=state:{str(self.state_code).zfill(2)}&in=county:{str(self.county_code)}&in=tract:{','.join([str(t) for t in self.tracts[i:i+30]])}" for j in range(0, len(self.tables), 30)]

    def get(self):
        dfs = []
        
        for url_list in tqdm(list(self.get_batched_queries())):
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


class CensusData:
    DATA_SOURCE = "2021/acs/acs5"
    BLOCK_GROUP_YEAR = "2020"

    def __init__(self, tables_file, groupings_file = None, geo_cache = "census_geo.cache", data_cache = "census_data.cache"):
        self.tables_file = tables_file
        self.groupings_file = groupings_file

        self.tract_list = defaultdict(list)
        self.location_list = {}
        self.data = pd.DataFrame(columns=Query.GEO_FIELDS)

        self.data_cache = data_cache
        self.geo_cache = geo_cache
        
        if Path(geo_cache).is_file():
            with open(geo_cache) as f:
                geo_data = json.load(f)
                location_list = geo_data["location_list"]
                tract_list = geo_data["tract_list"]
                self.location_list = {tuple(map(float, k.split(","))):v for k,v in location_list.items()}
                self.tract_list = {tuple(k.split(",")):v for k,v in tract_list.items()}

        if Path(data_cache).is_file():
            self.data = pd.read_csv(data_cache, index_col=0)

    def cache(self):
        self.data.to_csv(str(self.data_cache))

        location_list_cache = {f"{k[0]},{k[1]}":v for k,v in self.location_list.items()}
        tract_list_cache = {f"{k[0]},{k[1]}":v for k,v in self.tract_list.items()}
        geo_data = {}
        geo_data["location_list"] = location_list_cache
        geo_data["tract_list"] = tract_list_cache
        with open(str(self.geo_cache), "w+") as f:
            json.dump(geo_data, f)

    def location_request(self, latitude, longitude):
        response = requests.get(f"https://geo.fcc.gov/api/census/block/find?latitude={latitude}&longitude={longitude}&censusYear={CensusData.BLOCK_GROUP_YEAR}&format=json")
        data = response.json()
        state_code = data["Block"]["FIPS"][:2]
        county_code = data["Block"]["FIPS"][2:5]
        tract_code = data["Block"]["FIPS"][5:11]
        block_code = data["Block"]["FIPS"][11]
        return state_code, county_code, tract_code, block_code 
    
    def add_location(self, point: Point):
        longitude, latitude = point.x, point.y
        state_code, county_code, tract_code, block_code = self.location_request(latitude, longitude)
        self.tract_list[(state_code, county_code)].append(tract_code)
        self.location_list[(latitude, longitude)] = (state_code, county_code, tract_code, block_code[-1])

    def add_locations_from_geodataframe(self, gdf):
        for _, point in tqdm(gdf.geometry.items(), total=len(gdf.geometry)):
            if (point.y, point.x) not in self.location_list:
                self.add_location(point)

        self.cache()

    def lookup_location(self, point: Point):
        longitude, latitude = point.x, point.y
        if (latitude, longitude) not in self.location_list:
            print(f"Location {(latitude, longitude)} not found, adding this location.")
            self.add_location(latitude, longitude)
            self.download_data()

        state_code, county_code, tract_code, block_code =  self.location_list[(latitude, longitude)]
        df = self.data[(self.data.state == int(state_code)) & (self.data.county == int(county_code)) & (self.data.tract == int(tract_code)) & (self.data['block group'] == int(block_code))]
        return df.iloc[0].to_dict()
    
    def get_all_location_data(self, download=False):
        if download:
            self.download_data()

        dfs = []
        for (latitude, longitude) in self.location_list:
            state_code, county_code, tract_code, block_code =  self.location_list[(latitude, longitude)]
            df = self.data[(self.data.state == int(state_code)) & (self.data.county == int(county_code)) & (self.data.tract == int(tract_code)) & (self.data['block group'] == int(block_code))]
            if 'geometry' not in df.columns:
                df['geometry'] = Point(longitude, latitude)
            dfs.append(df)
        
        return gpd.GeoDataFrame(pd.concat(dfs))
         
    
    def download_data(self):
        dfs = []
        for (state_code, county_code) in self.tract_list:
            query = Query(CensusData.DATA_SOURCE, state_code, county_code)
            query.add_tables_from_yaml(self.tables_file)
            if self.groupings_file:
                query.add_groupings_from_yaml(self.groupings_file)
            for tract in self.tract_list[(state_code, county_code)]:
                query.add_tract(tract)

            dfs.append(query.get())

        self.data = pd.concat(dfs)
        self.cache()


