import argparse
import os
import subprocess
import yaml
from urllib.request import Request
from urllib.parse import urlencode, urlparse, parse_qs
from datetime import datetime

def run_command(command):
    return subprocess.run(command, shell=True, check=True, text=True, stdout=subprocess.PIPE).stdout

def format_req(url, key = None):
    if not key:
        return url
    
    parsed_url = urlparse(url)
    query_parameters = {'api_key': key, 'token': key }
    existing_query_params = parse_qs(parsed_url.query)
    existing_query_params.update(query_parameters)
    new_query = urlencode(existing_query_params, doseq=True)
    new_url = parsed_url._replace(query=new_query).geturl()
    
    req = Request(new_url)
    req.add_header('Authorization', key)
    
    return req.get_full_url()

def main():
    parser = argparse.ArgumentParser(description="Python equivalent of a Bash script")
    parser.add_argument("config_file", help="Configuration file")
    args = parser.parse_args()

    config_file = args.config_file

    if not os.path.isfile(config_file):
        print(f"Error: Configuration file '{config_file}' not found.")
        exit(1)

    with open(config_file, 'r') as file:
        config_data = yaml.safe_load(file)

    data_dir = config_data.get("data_dir", "data")
    name = config_data.get("name")
    gtfs_filename = config_data.get("gtfs_filename")
    gtfs_url = config_data.get("gtfs_url")
    sqlite_db = config_data.get("sqlite_db")
    realtime_url = config_data.get("realtime_url")
    tz = config_data.get("timezone")
    requires_key = config_data.get("requires_key", False)
    api_key = ""

    if requires_key:
        api_key_file = config_data.get("api_key_file")
        if api_key_file and os.path.isfile(api_key_file):
            api_key = yaml.safe_load(open(api_key_file))['key']

    os.makedirs(data_dir, exist_ok=True)

    save_log_file = os.path.join(data_dir, f"save_log.txt")
    
    if not os.path.isfile(save_log_file):
        run_command(f'wget -O {gtfs_filename} "{format_req(gtfs_url, key = api_key)}"')
        run_command(f"python3 gtfs/gtfs_import.py -f {gtfs_filename} -d sqlite:///{sqlite_db}")
    else:
        print("Found save log file")

    with open(save_log_file, "a+") as f:
        print("Starting: ", datetime.now(), file=f)

    try:
        run_command(f"python3 gtfs/gtfs_realtime.py -f {data_dir}/log.log -t {tz} -u {realtime_url} {'-x ' + api_key if api_key else ''} -d sqlite:///{sqlite_db} -c -v")
    except Exception as e:
        print(e)
        with open(save_log_file, "a+") as f:
            print("Ending: ", datetime.now(), file=f)

if __name__ == "__main__":
    main()
