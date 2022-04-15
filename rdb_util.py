import os

import pandas as pd
from sqlalchemy import MetaData, text
from sqlalchemy import create_engine
from collections import defaultdict


def crawl_db(engine) -> dict:
    
    # Gather the table data from the database

    # This is the directory where we will temporarily store csv files for the model
    base_path = "./"

    metadata = MetaData()
    metadata.reflect(engine)

    rdb_config = {}
    rdb_config["table_data"] = {}
    rdb_config["table_files"] = {}

    for name, table in metadata.tables.items():
        df = pd.read_sql_table(name, engine)
        rdb_config["table_data"][name] = df
        filename = name + ".csv"
        df.to_csv(filename, index=False, header=True)
        rdb_config["table_files"][name] = base_path + filename

# Extract primary/foriegn key relationshihps

    rels_by_pkey = defaultdict(list)

    for name, table in metadata.tables.items():
        for col in table.columns:
            for f_key in col.foreign_keys:
                rels_by_pkey[(f_key.column.table.name, f_key.column.name)].append((name, col.name))

    list_of_rels_by_pkey = []

    for p_key, f_keys in rels_by_pkey.items():
        list_of_rels_by_pkey.append([p_key] + f_keys)

    rdb_config["relationships"] = list_of_rels_by_pkey
    
# Extract each table's primary key

    rdb_config["primary_keys"] = {}
    for name, table in metadata.tables.items():
        for col in table.columns:
            if col.primary_key:
                rdb_config["primary_keys"][name] = col.name
                
    return rdb_config


def save_to_rdb(orig_db:str, new_db:str, transformed_tables:dict, engine, type="sqlite"):
    
    if type == "sqlite":
        
        cmd = "cp " + orig_db + ".db " + new_db + ".db"
        os.system(cmd) 
        connect_string = "sqlite:///" + new_db + ".db"
        engine_xf = create_engine(connect_string)

        with engine_xf.connect() as conn:

            for table in transformed_tables:
                command = "DELETE FROM " + table
                conn.execute(command)
                transformed_tables[table].to_sql(table, con=conn, if_exists='append', index=False)

    elif type == "postgres":

        with engine.connect() as conn:
            sql = "create database " + new_db + " with templates " + orig_db
            result = conn.execute(text(sql))
            connect_string = "postgres:///" + new_db + ".db"
            engine_xf = create_engine(connect_string)
            for table in transformed_tables:
                transformed_tables[table].to_sql(table, con=engine_xf, if_exists='replace', index=False)

            conn.commit()
            
    else:
        print("Type " + type + " is not supported yet")
              