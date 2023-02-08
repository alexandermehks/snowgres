import sys
import time

class SnowFlakeHealthCheck():
    def __init__(self, snowflake):
        self.max_connection_attempts = 5
        self.parent = snowflake
        print("INFO: Snowflake healthchecker started. \n")

    def _assert_connection(self):
        self.count_attempts()
        try:
            cur = self.parent.connection.cursor()
            cur.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS")
            self.parent.connection_attempts = 0
            return True
        except Exception as error:
            print("---------------------")
            print(f"Stacktrace: {error}")
            self.parent.connection_attempts += 1
            print(f"Trying to establish connection to snowflake {self.parent.connection_attempts}/{self.max_connection_attempts}\n")
            print("---------------------")
            self.parent.connect()
            return False

    def count_attempts(self):
        if self.parent.connection_attempts > 0:
            time.sleep(5)
        if self.parent.connection_attempts == self.max_connection_attempts:
            print("Max connection attemts reached, closing down.")
            sys.exit()

#-----------------------------------------------------------------------#

class PostgresHealthCheck():
    def __init__(self, postgres):
        self.max_connection_attempts = 5
        self.parent = postgres
        print("INFO: Postgres healthchecker started. \n")

    def _assert_connection(self):
        self.count_attempts()
        try:
            cur = self.parent.connection.cursor()
            cur.execute("SELECT * FROM information_schema.tables")
            self.parent.connection_attempts = 0
            return True
        except Exception as error:
            print("---------------------")
            print(f"Stacktrace: {error}")
            self.parent.connection_attempts += 1
            print(f"Trying to establish connection to postgres {self.parent.connection_attempts}/{self.max_connection_attempts}\n")
            print("---------------------")
            self.parent.connect()
            return False

    def count_attempts(self):
        if self.parent.connection_attempts > 0:
            time.sleep(5)
        if self.parent.connection_attempts == self.max_connection_attempts:
            print("Max connection attemts reached, closing down.")
            sys.exit()
