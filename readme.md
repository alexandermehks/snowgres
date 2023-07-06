## Dynamic integration from snowflake to postgres

Importing data from snowflake and inserting it into postgres database. It was written under 2 days since a problem came up that needed to be solved quickly. It works as it should, but could need some refactoring.

## JSON as input
The program takes JSON data as input which looks like this: 
```json
{
    "select_snowflake_columns": {
        "snow_column_1": "pg_column_1"
    },
    "to_postgres_table": "pg_table",
    "postgres_pk": [
        "PK1",
        "PK2",
        "PK3"
    ],
    "from_snowflake_table": "SNOWFLAKE_TABLE",
    "days_back": "5"
}
```
Currently the input are fetched from the postgres database that data should be written to, even tho any input would be valid as long as it is valid json.
