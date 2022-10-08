from typing import Any
from logging import Logger
import numpy as np
import os
import glob
import psycopg2
import pandas as pd
from model.sql_queries import time_table, user_table, artist_table, song_table, songplays_table
from config import constants as C
from repository.TimeRepository import TimeRepository
from repository.UserRepository import UserRepository
from repository.ArtistRepository import ArtistRepository
from repository.SongRepository import SongRepository


class Etl:
    def __init__(self, logging: Logger):
        self.user_repo = UserRepository(logging)
        self.time_repo = TimeRepository(logging)
        self.artist_repo = ArtistRepository(logging)
        self.song_repo = SongRepository(logging)

    def process_song_file_(self, filepath: str):
        """
        Processes a single song file.
        @param cur: the database cursor
        @param filepath: the path to the song file
        """
        # open song file
        df = pd.read_json(filepath, lines=True)

        # insert artist record
        artist_data = df[artist_table["data_fields"]]
        artist_data.columns = artist_table["fields"]
        self.artist_repo.store_records(list(artist_data.T.to_dict().values()))

        # insert song record
        song_data = df[song_table["data_fields"]]
        song_data.columns = song_table["fields"]
        self.song_repo.store_records(list(song_data.T.to_dict().values()))

    def process_log_file_(self, filepath: str):
        """
        Processes a single log file.
        @param cur: the database cursor
        @param filepath: the path to the log file
        """
        # open log file
        df = pd.read_json(filepath, lines=True)

        # convert timestamp column to datetime
        df["ts"] = pd.to_datetime(df["ts"], unit='ms')

        # get all the wanted information from the timestamps
        timestamps = df["ts"]
        hours = df["ts"].dt.hour
        days = df["ts"].dt.day
        weeks = df["ts"].dt.week
        months = df["ts"].dt.month
        years = df["ts"].dt.year
        weekdays = df["ts"].dt.weekday

        # create a dataframe with the wanted information
        time_data = pd.DataFrame(
            {"timestamp": timestamps, "hour": hours, "day": days, "week": weeks, "month": months, "year": years,
             "weekday": weekdays})
        time_data.columns = time_table["fields"]
        self.time_repo.store_records(list(time_data.T.to_dict().values()))

        # load user table
        user_data = df[user_table["data_fields"]]
        user_data.columns = user_table["fields"]
        user_data = user_data.drop_duplicates()
        user_data.dropna(inplace=True)
        records = list(user_data.T.to_dict().values())
        self.user_repo.store_records(records)



        # # insert songplay records
        # for index, row in df.iterrows():
        #
        #     # get songid and artistid from song and artist tables
        #     cur.execute(song_select, (row.song, row.artist, row.length))
        #     results = cur.fetchone()
        #
        #     if results:
        #         songid, artistid = results
        #     else:
        #         songid, artistid = None, None
        #
        #     # insert songplay record
        #     songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        #     cur.execute(songplay_table_insert, songplay_data)


    def process_data(self, filepath: str, log):
        """
        Processes either logs or songs depending on the given function.
        @param cur: the database cursor
        @param conn: the database connection
        @param filepath: the path to the data directory
        @param func: the function (process songs or logs)
        """
        # get all files matching extension from directory
        all_files = []
        for root, dirs, files in os.walk(filepath):
            files = glob.glob(os.path.join(root, '*.json'))
            for f in files:
                all_files.append(os.path.abspath(f))

        # get total number of files found
        num_files = len(all_files)
        print('{} files found in {}'.format(num_files, filepath))

        # iterate over files and process
        for datafile in all_files:
            if log:
                self.process_log_file_(datafile)
            else:
                self.process_song_file_(datafile)
