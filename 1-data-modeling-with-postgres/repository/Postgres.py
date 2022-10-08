from logging import Logger

import psycopg2
import psycopg2.extras
from psycopg2 import sql

from config import constants as C
from typing import Dict, List, Any, Tuple
import os
import time


class Postgres:
    def __init__(self, logger: Logger):
        """
        Initialize object with connection and logger from getting credentials from conifg.constants

        :param logger: Logger: to log all events
        """
        self.logger: Logger = logger
        self.write_connection = psycopg2.connect(host=C.host,
                                                     database=C.dbname,
                                                     user=C.username,
                                                     password=C.password,
                                                     options='-c statement_timeout=10000')

        self.read_connection = psycopg2.connect(host=C.host,
                                                     database=C.dbname,
                                                     user=C.username,
                                                     password=C.password,
                                                     options='-c statement_timeout=10000')

        self.write_connection.autocommit = True
        self.read_connection.autocommit = True

    def batch_put_records(self,  table_name: str, keys: List[str], fields: List[str],
                          records: List[Dict[str, Any]]):
        """
        Function to batch insert records to a table of a schema

        :param schema_name: str: name of the schema
        :param table_name: str: name of the table
        :param keys: List[str]: list of keys
        :param fields: List[str]: list of other fields in that table
        :param records: List[Dict[str,Any]]: list of record values
        :return:
        """
        start_time: float = time.time()
        # query to insert to table
        insert_query = sql.SQL("""INSERT INTO {table} ({fields}) 
                                  VALUES {values_placeholder} 
                                  ON CONFLICT ({keys}) DO NOTHING """) \
            .format(
                    table=sql.Identifier(table_name),
                    fields=sql.SQL(',').join(
                        [sql.Identifier(f) for f in
                         fields]),
                    keys=sql.SQL(', ').join(
                        [sql.Identifier(f) for f in keys]),
                    values_placeholder=sql.Placeholder())
        # template for record list
        record_list_template: str = "(" + ','.join(['%s'] * (len(fields))) + ")"

        # record tuples
        record_tuples: List[Tuple] = [tuple([r.get(f) for f in fields]) for r in records]

        cursor = self.write_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        try:
            psycopg2.extras.execute_values(cursor,
                                           insert_query.as_string(self.write_connection),
                                           record_tuples,
                                           template=record_list_template,
                                           page_size=C.db_batch_size)
            # self.write_rds_connection.commit()
        except psycopg2.DatabaseError as e:
            self.logger.error("Error while getting records : {0}\nQuery:{1}".format(e, cursor.query.decode('utf-8')))
            # self.write_rds_connection.rollback()
            raise e
        finally:
            if cursor:
                cursor.close()

        end_time: float = time.time()
        diff: float = (end_time - start_time) * 1000
        self.logger.debug("Time taken to insert {} records to {}: {:.2f}ms".format(len(records), table_name, diff))
        return True

    def get_records(self, sql_query: sql, parameters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Function to get records using select query

        :param sql_query: sql: sql query to retrieve records
        :param parameters: Dict[str, Any]: dict of parameters for values in the query
        :return: List[Dict[str, Any]]: list of records
        """
        start_time: float = time.time()
        cursor = self.read_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        results: List[Dict[str, Any]] = []
        try:
            cursor.execute(sql_query, parameters)

            rows = cursor.fetchall()
            if rows:
                rows = [dict(row) for row in rows]
                results.extend(rows)
            # self.read_rds_connection.commit()
        except psycopg2.DatabaseError as e:
            self.logger.error("Error while getting records : {0}\nQuery:{1}".format(e, cursor.query.decode('utf-8')))
            self.logger.error(sql_query)
            self.logger.error(parameters)
            # self.read_rds_connection.rollback()
            raise e
        finally:
            if cursor:
                cursor.close()
        end_time: float = time.time()
        diff: float = (end_time - start_time) * 1000
        self.logger.debug("Time taken to get {0} records: {1:.2f}ms\nQuery:{2}".format(len(results), diff, cursor.query.decode('utf-8')))

        return results

    def get_record(self, sql_query: sql, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Function to get a single record

        :param sql_query: sql: sql query to get a record
        :param parameters: Dict[str, Any]: dict of parameters for values in the query
        :return: Dict[str, Any]: one record
        """
        start_time: float = time.time()
        cursor = self.read_rds_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        try:

            cursor.execute(sql_query, parameters)
            row = cursor.fetchone()
            end_time: float = time.time()
            diff: float = (end_time - start_time) * 1000
            self.logger.debug("Time taken to get a record: {0:.2f}ms\nQuery:{1}".format(diff, cursor.query.decode('utf-8')))
            # self.read_rds_connection.commit()
            return row
        except psycopg2.DatabaseError as e:
            self.logger.error("Error while getting records : {0}\nQuery:{1}".format(e, cursor.query.decode('utf-8')))
            self.logger.error(sql_query)
            self.logger.error(parameters)
            # self.read_rds_connection.rollback()
            raise e
        finally:
            if cursor:
                cursor.close()