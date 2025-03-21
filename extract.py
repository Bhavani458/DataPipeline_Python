import requests
import pandas as pd # type: ignore
from datetime import datetime, timedelta
import os

def process_earthquake_data():
    today_date = datetime.now()
    date_15_days_ago = today_date - timedelta(days=15)
    start_time = date_15_days_ago.strftime("%Y-%m-%d")
    end_time = today_date.strftime("%Y-%m-%d")

    url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start_time}&endtime={end_time}"

    response = requests.get(url)

    if response.status_code==200:

        data= response.json()
        features = data['features']

        earthquakes = []

        date = today_date.strftime("%Y_%m_%d")

        filepath = f"/Users/bhavanipriya/Documents/DataPipeline_Python/data/earthquake_{date}.csv"

        for feature in features:
            properties = feature["properties"]
            geometry = feature["geometry"]
            earthquake = {
                'time' : properties['time'],
                'place' : properties['place'],
                'magnitude' : properties['mag'],
                'longitude' : geometry['coordinates'][0],
                'latitude': geometry['coordinates'][1],
                'depth' : geometry['coordinates'][2],
                'file_path' : filepath
            }

            earthquakes.append(earthquake)

            df = pd.DataFrame(earthquakes)

            if os.path.exists(filepath):
                os.remove(filepath)
                print(f"File {filepath} removed")

            df.to_csv(filepath,index=False)
            print(f"File {filepath} created and written to")
        else:
            print(f"Failed to retrieve data {response.status_code}")

def main():
    process_earthquake_data()

if __name__ == "__main__":
    main()
    


