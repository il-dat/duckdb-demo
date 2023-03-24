import sys
from utils import yaml
from pathlib import Path
import snowflake.connector as sf
import pandas as pd
import duckdb

DBT_PROFILE_DIR = str(Path.home() / ".dbt")
DBT_PROFILE_NAME = "duckdbdemo"
EXPORT_TABLE_NAME = sys.argv[1] if len(sys.argv) > 1 else "--required--"

# Read config from dbt profiles.yml
config_dict = None
with open(f"{DBT_PROFILE_DIR}/profiles.yml", 'r') as file:
    config_dict = yaml.load_yaml_text(file.read())
config_dict = config_dict.get(DBT_PROFILE_NAME, {}).get("exporter", {}).get("snowflake", None)

# Export table configured at args[0]
print("Table:", EXPORT_TABLE_NAME)
with sf.connect(**config_dict) as conn:
    data = pd.read_sql(sql=f"select * from {EXPORT_TABLE_NAME}", con=conn)
print("--data:", data)

# Write to Duck DB
db_file = f"./streamlit/{DBT_PROFILE_NAME}.duckdb"
with duckdb.connect(db_file) as duckdb_conn:
    existed = not duckdb_conn\
        .sql(f"select * from information_schema.tables where table_name='{EXPORT_TABLE_NAME}'")\
        .to_df().empty
    if existed:
        duckdb_conn.sql(f"truncate table {EXPORT_TABLE_NAME}")
    else:
        duckdb_conn.sql(f"create table {EXPORT_TABLE_NAME} as select * from data")
    duckdb_conn.sql(f"insert into {EXPORT_TABLE_NAME} select * from data")
    
    duckdb_conn.table(EXPORT_TABLE_NAME).show()
print("Duck DB created/updated:", db_file)