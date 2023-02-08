import sys
import os 
import snowflake.connector
from datetime import date
from datetime import datetime
from .maintenance import HealthCheck

class SnowFlake:
    def __init__(self):
        self.state = None
        self.connection = None
        self.connection_attempts = 0
        self.health = HealthCheck.SnowFlakeHealthCheck(self)
        self.connect()

    def connect(self):
        try:
            connection = snowflake.connector.connect(
                    user=os.environ["SNOWFLAKE_USER"],
                    password=os.environ["SNOWFLAKE_PASSWORD"],
                    account=os.environ["SNOWFLAKE_ACCOUNT"],
                    database=os.environ["SNOWFLAKE_DATABASE"])
            self.state = "Connected"
            self.connection = connection
            print("INFO: Successfully connected to Snowflake database.\n")
        except Exception as error:
            print(error)

    def healthcheck(self):
        if not self.health._assert_connection():
            eval("self."+sys._getframe().f_back.f_code.co_name+"()")
            return False
        return True

    def fetch_tables(self):
        if self.healthcheck() == True:
            try:
                cur = self.connection.cursor()
                cur.execute(f"SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS")
                result = cur.fetchall()
                return result
            except Exception as error:
                print(f"Error in fetch_data: {error}")

    def table_metadata(self, table):
        if self.healthcheck() == True:
            #tables = self.fetch_tables()
            schema, name = table.split(".")
            cur = self.connection.cursor()
            cur.execute(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='{schema}' AND TABLE_NAME='{name}' ORDER BY ORDINAL_POSITION")
            res = cur.fetchall()
            return res

    def fetch_table(self, table, days_back,columns):
        if self.healthcheck() == True:
            try:
                cols = ""
                for col in columns:
                    cols += col+", "
                cols = cols[:-2]
                columns = self.build_columns(columns)
                cur = self.connection.cursor()
                cur.execute(f"SELECT {cols} FROM {table} WHERE MODIFIED_TIMESTAMP > '{days_back}'")
                result = cur.fetchall()
                return result
            except Exception as error:
                print(f"Error in fetch_data: {error}")

    def build_columns(self, columns):
        buf = ""
        for col in columns:
            buf += (col + ",")
        buf = buf[:-1]
        return buf
