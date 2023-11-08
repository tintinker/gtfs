import argparse
import yaml
from pathlib import Path

config_dir = Path("config")
api_key_dir = config_dir / "api_keys"

config_dir.mkdir(parents=True, exist_ok=True)
api_key_dir.mkdir(parents=True, exist_ok=True)


# Function to calculate derived arguments
def calculate_derived_args(args):
    data_dir = f"data/{args.name}"
    sqlite_db = f"{data_dir}/{args.name}.db"
    gtfs_filename = f"{data_dir}/{args.name}_gtfs.zip"
    cache_filename = f"{data_dir}/cache.csv"
    graph_filename = f"{data_dir}/{args.name}_graph.json"
    shp_folder =  f"{data_dir}/shp"
    return data_dir, sqlite_db, gtfs_filename, cache_filename, graph_filename, shp_folder

parser = argparse.ArgumentParser(description="Generate a YAML configuration file based on provided arguments.")
parser.add_argument("-n", "--name", required=True, help="Name of the configuration")
parser.add_argument("-g", "--gtfs_url", required=True, help="GTFS URL")
parser.add_argument("-r", "--realtime_url", required=True, help="Realtime URL")
parser.add_argument("-t", "--timezone", required=True, help="Timezone UTC Offset (-8 for Pacific Time)")
parser.add_argument("-a", "--api_key", help="API Key (optional)")

try:
    args = parser.parse_args()
except:
    parser.print_help()
    print("Invalid options")
    print()

    print("###########################################")
    print("Starting interactive mode")
    print("############################################")
    
    print()
    print()

    args = argparse.Namespace()
    args.name = input("Name of the configuration: ").strip()
    args.gtfs_url = input("GTFS URL: ").strip()
    args.realtime_url = input("Realtime URL: ").strip()
    args.timezone = int(input("Timezone UTC Offset (-8 for Pacific Time): "))
    args.api_key = input("API Key (press enter to skip): ").strip()





# Calculate derived arguments
data_dir, sqlite_db, gtfs_filename, cache_filename, graph_filename, shp_folder = calculate_derived_args(args)

requires_key = False
api_key_output_file = ""

if args.api_key:
    requires_key = True
    api_key_data = {"key": args.api_key}
    api_key_output_file = api_key_dir / f"{args.name}.yaml"
    
    with open(api_key_output_file, "w+") as api_key_file:
        yaml.dump(api_key_data, api_key_file, default_flow_style=False)
    
    print(f"API Key YAML configuration saved to {api_key_output_file}")
    


# Create a dictionary with the provided and calculated arguments
config_data = {
    "name": args.name,
    "data_dir": data_dir,
    "sqlite_db": sqlite_db,
    "gtfs_filename": gtfs_filename,
    "cache_filename": cache_filename, 
    "graph_filename": graph_filename,
    "gtfs_url": args.gtfs_url,
    "realtime_url": args.realtime_url,
    "timezone": args.timezone,
    "requires_key": requires_key,
    "api_key_file": str(api_key_output_file) if api_key_output_file else "",
    "shp_folder": shp_folder
}



# Specify the output file path for the main configuration
output_file = config_dir / f"{args.name}.yaml"

# Convert the dictionary to YAML format and write it to the output file
with open(output_file, "w+") as file:
    yaml.dump(config_data, file, default_flow_style=False)

print(f"Main YAML configuration saved to {output_file}")

