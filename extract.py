import requests #library to interact with urls or links
import pandas as pd # type: ignore #data manipulation
from datetime import datetime, timedelta
import os

#function to get content
def process_earthquake_data():
    #today's date
    today_date = datetime.now()
    #date 15days ago
    date_15_days_ago = today_date - timedelta(days=15)
    #define start time
    start_time = date_15_days_ago.strftime("%Y-%m-%d")
    #define end time
    end_time = today_date.strftime("%Y-%m-%d")

    url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start_time}&endtime={end_time}"

    #gets response
    response = requests.get(url)

    #if code is 200 then proceed
    if response.status_code==200:
        #gets json data
        data= response.json()

        #features field of data
        features = data['features']

        #empty list
        earthquakes = []

        date = today_date.strftime("%Y_%m_%d")

        filepath = f"/Users/bhavanipriya/Documents/DataPipeline_Python/data/earthquake_{date}.csv"

        #goes through each feature and gets data for each field
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

            #append the data to earthquakes
            earthquakes.append(earthquake)

            #converts to dataframe
            df = pd.DataFrame(earthquakes)

            if os.path.exists(filepath):
                os.remove(filepath)
                print(f"File {filepath} removed")

            #saves the file as csv
            df.to_csv(filepath,index=False)
            print(f"File {filepath} created and written to")
        else:
            print(f"Failed to retrieve data {response.status_code}")

def main():
    process_earthquake_data()

if __name__ == "__main__":
    main()
    


