import overpass
import geopandas as gpd
from tqdm import tqdm
from shapely.geometry import Point


class OpenStreetMapsData:
    def __init__(self, north_lat, west_lng, south_lat, east_lng) -> None:
        self.bbox = f"{north_lat},{west_lng},{south_lat},{east_lng}"
        self.api = overpass.API()

    def find_parks(self,return_geodataframe=True):
        query = f"""node["leisure"="park"]({self.bbox});"""
        return self.find("grocery", query, return_geodataframe)
    
    def find_grocery_store(self,return_geodataframe=True):
        query = f"""node["shop"="supermarket"]({self.bbox});"""
        return self.find("grocery", query, return_geodataframe)
    
    def find_worship(self, return_geodataframe=True):
        return self.find_amenity("place_of_worship", return_geodataframe)

    def find_bars(self, return_geodataframe=True):
        return self.find_amenity("bar", return_geodataframe)
    
    def find_hospitals(self, return_geodataframe=True):
        return self.find_amenity("hospital", return_geodataframe)

    def find_mcdonalds(self, return_geodataframe=True):
        return self.find_brand("McDonald's", return_geodataframe)

    def find_starbucks(self, return_geodataframe=True):
        return self.find_brand("Starbucks", return_geodataframe)
    
    def find_brand(self, brand, return_geodataframe=True):
        query = f"""node["brand"="{brand}"]({self.bbox});"""
        return self.find(brand, query, return_geodataframe)
    
    def find_amenity(self, amenity, return_geodataframe=True):
        query = f"""node["amenity"="{amenity}"]({self.bbox});"""
        return self.find(amenity, query, return_geodataframe)

    def find(self, name, query, return_geodataframe=True):
        tqdm.write("Performing OSM Query: " + query)
        response = self.api.Get(query)
        
        results = []
        for element in response['features']:
            results.append({
                'osm_id': element['id'],
                'osm_name': element['properties'].get('name', 'Unknown Name'),
                'geometry': Point(element['geometry']['coordinates'][0], element['geometry']['coordinates'][1]),
                'osm_query': name
            })

        if not return_geodataframe:
            return results
        
        return gpd.GeoDataFrame(results)



        
        



