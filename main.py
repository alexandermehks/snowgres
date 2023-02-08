import sys
from development import environment
from src.SnowFlake import SnowFlake
from src.Postgres import Postgres
import datetime

def data_builder(data, metadata, pg_where):
    row_buf = []
    for _ in data:
        b = []
        for index,key in enumerate(pg_where):
            for metakey in metadata:
                if key == metakey[0]:
                    zipped = ([_[index], metakey])
                    b.append(zipped)
        row_buf.append(b)
    return row_buf

if __name__ == "__main__":
    environment.is_development()

    current_date = datetime.datetime.now()

    #Create postgres database and fetch metadata about the jobs.
    postgres = Postgres()
    job_metadata = postgres.jobs
    if not job_metadata:
        print(f"INFO: No active jobs found. Gracefully shutting down. {current_date}")
        sys.exit()
    
    #Create snowflake instance and fetch data based on the metadat  
    snowflake = SnowFlake()

    for job in job_metadata:
        time_span = current_date - datetime.timedelta(int(job["days_back"]))

        #Postgres stuff
        postgres_table = job["to_postgres_table"]
        postgres_insert_where = job["insert_postgres_columns"]

        #Snowflake stuff
        snowflake_columns = job["select_snowflake_columns"]["columns"]
        snowflake_table = job["from_snowflake_table"]


        snowflake_data = snowflake.fetch_table(snowflake_table, time_span, snowflake_columns)
        snowflake_metadata = snowflake.table_metadata(snowflake_table)
        
        data_x_type = data_builder(snowflake_data, snowflake_metadata,postgres_insert_where)

        postgres.prepare_queries(data_x_type, postgres_insert_where, postgres_table)
