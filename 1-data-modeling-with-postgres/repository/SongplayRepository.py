from logging import Logger
from typing import List, Dict, Any
from repository.Postgres import Postgres
import config.constants as C
from model.sql_queries import songplays_table
import os


class SongplayRepository():
    def __init__(self, logger: Logger):
        self.logger: Logger = logger
        # creating a different connection for Anonymous,
        self.dbname = C.dbname
        self.postgres: Postgres = Postgres(logger)
        self.table_name: str = songplays_table["table_name"]
        self.fields: List[str] = songplays_table["fields"]
        self.keys: List[str] = songplays_table["keys"]

    def store_records(self, records: List[Any]):
        """
        Function to store sophi events for Anonymous Users

        :param user_journeys: List[Any]: list of anonymous user journey objects
        :return:
        """
        self.postgres.batch_put_records(
                                       table_name=self.table_name,
                                       keys=self.keys,
                                       fields=self.fields,
                                       records=records)
    def get_events(self):
        pass

    def get_sessions(self):
        pass