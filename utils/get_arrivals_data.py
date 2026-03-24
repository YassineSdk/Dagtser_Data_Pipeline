import requests 
import pandas as pd 
from dotenv import load_dotenv
import os


load_dotenv(".env")
key = os.getenv("API_KEY")

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

def fetch_arrival_data():
    """this fuctions feaches arrivals data related to the stations"""
    collection = []

    for naptan_id , station in STATIONS.items():
        try:
            url = f"api.tfl.gov.uk/StopPoint/{naptan_id}/arrivals"
            r = requests.get(url,params= {"app_key":key})
            r = r.json()
            collection.append(r)
        except Exception as e:
            print(f"error getting data for {station} : {e}")
        continue

    return pd.DataFrame(collection) 

data = fetch_arrival_data()
print(data)