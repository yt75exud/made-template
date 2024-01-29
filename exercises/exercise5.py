import zipfile
import urllib.request
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float

zip = "https://gtfs.rhoenenergie-bus.de/GTFS.zip"
zip_path = "GTFS.zip"
filename = "stops.txt"

urllib.request.urlretrieve(zip, zip_path)
with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extract(filename)

#Only the columns stop_id, stop_name, stop_lat, stop_lon, zone_id with fitting data types
columns = ["stop_id", "stop_name", "stop_lat", "stop_lon", "zone_id"]

data = pd.read_csv(filename, usecols=columns)

# Filtering data for only stops from 2001
data = data[data["zone_id"] == 2001]

# Validating data
# Text validation for stop_name
data["stop_name"] = data["stop_name"].astype(str)

# Geographic coordinates validation for stop_lat and stop_lon
valid_coord_range = (-90, 90)
valid_lat_range = data["stop_lat"].between(*valid_coord_range)
valid_lon_range = data["stop_lon"].between(*valid_coord_range)
data = data[valid_lat_range & valid_lon_range]

# Use fitting SQLite types & Write data into SQLite database
engine = create_engine('sqlite:///gtfs.sqlite')
metadata = MetaData()
stops_table = Table('stops', metadata,
                    Column('stop_id', Integer, primary_key=True),
                    Column('stop_name', String),
                    Column('stop_lat', Float),
                    Column('stop_lon', Float),
                    Column('zone_id', Integer))

metadata.create_all(engine)

data.to_sql('stops', con=engine, if_exists='replace', index=False, dtype={
    'stop_id': Integer,
    'stop_name': String,
    'stop_lat': Float,
    'stop_lon': Float,
    'zone_id': Integer
})
