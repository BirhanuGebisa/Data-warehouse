import sys
import pandas as pd
from sqlalchemy import text
import json
from sqlalchemy import create_engine
import numpy as np

engine = create_engine('postgresql+psycopg2://airflow:airflow@5432/postgres')

SCHEMA = "data_load.sql"


def create_table():
    try:
        with engine.connect() as conn:
            for name in [SCHEMA]:
                
                with open(f'/opt/sql_data/{name}', "r") as file:
                    query = text(file.read())
                    conn.execute(query)
        print("Successfull")
    except Exception as e:
        print("Error creating table",e)
        sys.exit(e)

def insert_to_table(json_stream :str, table_name: str,from_file=False ):
    try:
        if not from_file:
            df = pd.read_json(json_stream)
        else:
            with open(f'../temp/{json_stream}','r') as file:
                data=file.readlines()
            dt=data[0]

            df=pd.DataFrame.from_dict(json.loads(dt))
            df.columns=df.columns.str.replace(' ','')
            df.dropna(inplace=True)
        with engine.connect() as conn:
            df.to_sql(name=table_name, con=conn, if_exists='append', index=False)

    except Exception as e:
        print(f"error insert to table: {e}")  
        sys.exit(e)