import os
import sys
import json
import psycopg2 as pg
from .maintenance import HealthCheck

class Postgres():
    def __init__(self):
        self.database = os.environ["POSTGRES_DATABASE"]
        self.host= os.environ["POSTGRES_DATABASE_HOST"]
        self.user = os.environ["POSTGRES_DATABASE_USER"]
        self.password = os.environ["POSTGRES_DATABASE_PASSWORD"]
        self.port = os.environ["POSTGRES_DATABASE_PORT"]
        self.connection = None
        self.connection_attempts = 0
        self.health = HealthCheck.PostgresHealthCheck(self)
        self.connect()
        self.jobs = self.jobs()
    
    def connect(self):
        try:
            connection = pg.connect(database = self.database, host = self.host, user = self.user, password = self.password, port = self.port)
            self.connection = connection
            print("INFO: Successfully connected to Postgres database.\n")
        except Exception as e:
            print(f"Failed connecting to postgres database: {e}")
    
    def healthcheck(self):
        if not self.health._assert_connection():
            eval("self."+sys._getframe().f_back.f_code.co_name+"()")
            return False
        return True

    def get_jobs_metadata(self):
        if self.healthcheck() == True:
            try:
                cur = self.connection.cursor()
                cur.execute(f"SELECT job_metadata FROM snowflake_jobs WHERE active = True")
                result = cur.fetchall()
                return result
            except Exception as e:
                print(f"ERROR: Cant fetch metadata: {e}")

    def jobs(self):
        buf = []
        try:
            meta = self.get_jobs_metadata()
            for i in meta:
                json_convert = json.dumps(i[0])
                dict_convert = json.loads(json_convert)
                buf.append(dict_convert)
            return buf
        except Exception as e:
            print(f":ERROR: converting json data. {e}")

    def prepare_queries(self, data, where_to_insert, table):
        query_buf = []
        for row in data:
            b = []
            for column in row:
                b.append(self.data_converter(column))
            query_buf.append(b)
        self.query_builder(query_buf, where_to_insert,table)

    def query_builder(self, types, map, table):
        queries = []
        for row in types:
            base_sql = f"INSERT INTO {table} ("
            values = f" VALUES ("
            for column in row:
                base_sql += map[column[0]]+", "
                values += column[1]+", "
            base_sql = base_sql[:-2]+")"
            base_sql += values[:-2]+")"
            queries.append(base_sql)
        self.insert(queries)

    def data_converter(self, row):
        value = row[0]
        column_name = row[1][0]
        data_type = row[1][1]


        if data_type == "TEXT":
            return (column_name, self.text_type(value))

        elif data_type == "BOOLEAN":
            return (column_name, self.boolean_type(value))

        elif data_type == "TIMESTAMP_NTZ":
            return (column_name, self.date_type(value))

        return "'null', "
    
    def text_type(self, text):
        return f"'{text}'"

    def boolean_type(self, boolean):
        return f"'{boolean}'"
    
    def date_type(self, value):
        return f"'{value}'"+"::date"

    def insert(self, queries):
        if self.healthcheck() == True:
            try:
                cur = self.connection.cursor()
                for query in queries:
                    cur.execute(query)
                    self.connection.commit()
            except Exception as e:
                print(f"ERROR: Cant insert to database: {e}")
