import requests 
from dotenv import load_dotenv 
import os 
import pandas as pd 

STATIONS = {
    "940GZZLUKSX": "King's Cross St. Pancras",
    "940GZZLUVIC": "Victoria",
    "940GZZLULVT": "Liverpool Street",
    "940GZZLULDS": "London Bridge",
    "940GZZLUPAC": "Paddington",
    "940GZZLUBKF": "Blackfriars",
    "940GZZLUCST": "Cannon Street",
    "940GZZLUWSM": "Westminster",
    "940GZZLUWSP": "Waterloo",
    "940GZZLUBBN": "Barbican",
}

load_dotenv(".env")
key = os.getenv("API_KEY")

def get_raw_data():
    """this function gets realtime data from the Uk API """
    collection = []

    for id , station in STATIONS.items():
        try:
            url = f"https://api.tfl.gov.uk/crowding/{id}/Live"
            r = requests.get(url,params= {"app_key":key})
            r = r.json()
            r['station'] = station
            
            collection.append(r)
        except Exception as e:
            print(f"error getting data for {station} : {e}")
        continue

    data = pd.DataFrame(collection)
    len_data = data.shape
    available_data = data['dataAvailable'].value_counts()

    return data , len_data ,available_data 
