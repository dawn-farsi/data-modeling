from logging import Logger

import psycopg2
import psycopg2.extras
from psycopg2 import sql

from model.sql_queries import create_table_queries, drop_table_queries
import config.constants as C

class DbInfra:
    def __init__(self, logger: Logger):
        """
         Initialize object with connection and logger from getting credentials from conifg.constants

         :param logger: Logger: to log all events
         """
        self.logger : Logger = logger

        self.default_connection = psycopg2.connect(host=C.defaulthost,
                                                 database=C.defaultdbname,
                                                 user=C.username,
                                                 password=C.password,
                                                 options='-c statement_timeout=10000')

        self.default_connection.autocommit = True

    def create_database(self):
        """
        - Creates and connects to the database
        """
        # connect to default database
        cur = self.default_connection.cursor()

        drop_db_quary = sql.SQL("""DROP DATABASE IF EXISTS {dbname}""") \
            .format(dbname=sql.Identifier(C.dbname))

        create_db_query = sql.SQL("""CREATE DATABASE {dbname} WITH ENCODING 'utf8' TEMPLATE template0 """) \
            .format(dbname=sql.Identifier(C.dbname))

        try:
            cur.execute(drop_db_quary.as_string(self.default_connection))
            cur.execute(create_db_query.as_string(self.default_connection))
        except psycopg2.DatabaseError as e:
            self.logger.error("Error while creating database : {0}\nQuery:{1}".format(e, cur.query.decode('utf-8')))
            raise e
        finally:
            if self.default_connection:
                self.default_connection.close()

        # connect to sparkify database
        self.connection = psycopg2.connect(host=C.host,
                                           database=C.dbname,
                                           user=C.username,
                                           password=C.password,
                                           options='-c statement_timeout=10000')

        self.connection.autocommit = True

        self.cur = self.connection.cursor()


    def drop_tables(self):
        """
        Drops each table using the queries in `drop_table_queries` list.
        """
        try:
            for query in drop_table_queries:
                self.cur.execute(query)
        except psycopg2.DatabaseError as e:
            self.logger.error("Error while creating database : {0}\nQuery:{1}".format(e, self.cur.query.decode('utf-8')))
            raise e


    def create_tables(self):
        """
        Creates each table using the queries in `create_table_queries` list.
        @param cur:
        @param conn:
        """
        try:
            for query in create_table_queries:
                self.cur.execute(query)
        except psycopg2.DatabaseError as e:
            self.logger.error("Error while creating database : {0}\nQuery:{1}".format(e, self.cur.query.decode('utf-8')))
            raise e


    def main(self):
        """
        - Drops (if exists) and Creates the database.

        - Establishes connection with the database and gets
        cursor to it.

        - Drops all the tables.

        - Creates all tables needed.

        - Finally, closes the connection.
        """
        self.create_database()

        self.drop_tables()

        self.create_tables()

        self.connection.close()

