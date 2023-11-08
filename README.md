## Startup

1. Install requirements

```bash
pip3 install -r requirements.txt
```

2. Create config file (find gtfs and realtime urls on city website, may require requesting an api key) 

```bash
python3 config/generate_config.py

```
Name of the configuration: miami
GTFS URL: http://www.miamidade.gov/transit/googletransit/current/google_transit.zip
Realtime URL: https://api.goswift.ly/real-time/miami/gtfs-rt-trip-updates
Timezone UTC Offset (-8 for Pacific Time): -5
API Key (press enter to skip): [redacted]

3. Start downloader (leave running to get real time data for a day or so)
```bash
python3 download.py config/miami.yaml
```

4. Run analysis!

```bash
python3 analysis/graph.py config/miami.yaml
python3 analysis/shapefile.py  config/miami.yaml -s 30
```

Put into your fav visualizer