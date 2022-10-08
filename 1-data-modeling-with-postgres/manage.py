from repository.DbInfra import DbInfra
from repository.Postgres import Postgres
from config import constants as C
import pandas as pd
import os
import glob
from logging import Logger
from repository.Etl import Etl
from model.sql_queries import time_table, user_table, artist_table, song_table, songplays_table

# dbinfra = DbInfra(Logger("DBInfra"))
# dbinfra.main()

etl = Etl(Logger("Data Processing"))
etl.process_data("./data/log_data", log=True)
# etl.process_data("./data/song_data", log=False)
# all_files = []
# files = []
# for root, dirs, files in os.walk("./data/log_data"):
#     files = glob.glob(os.path.join(root, '*.json'))
#     for f in files:
#         all_files.append(os.path.abspath(f))
# # open song file
# df = pd.read_json(all_files[0], lines=True)
# print(df[user_table["data_fields"]])
#
# # insert song record
# song_data = df[["song_id", "title", "artist_id", "year", "duration"]]
# song_data = list(song_data.T.to_dict().values())
# print(song_data)
#
# artist_data = df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]]
# artist_data = list(artist_data.T.to_dict().values())
# artist_data=[{'artist_id': 'AR0RCMP1187FB3F427', 'name': 'Billie Jo Spears', 'location': 'Beaumont, TX', 'latitude': 30.08615, 'longitude': -94.10158}]
#
# postgre = Postgres(logger=Logger("data processing"))
# postgre.batch_put_records(schema_name=C.dbname,
#                           table_name="artists",
#                           keys=["artist_id"],
#                           fields=["artist_id", "name", "location", "lattitude", "longitude"],
#                           records=artist_data)
#
