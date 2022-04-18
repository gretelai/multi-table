import yaml
import numpy as np
import pandas as pd
from smart_open import open
from sklearn import preprocessing

from gretel_client import create_project
from gretel_client.helpers import poll


def create_model(rdb_config:dict, table:str, project, transform_policies:dict):

    # Read in the transform policy
    policy_file = transform_policies[table]
    yaml_file = open(policy_file, "r")
    policy = yaml_file.read()
    yaml_file.close()

    # Get the dataset_file_path
    dataset_file = rdb_config["table_files"][table]

    # Create the transform model
    model = project.create_model_obj(model_config=yaml.safe_load(policy))
    
    # Upload the training data.  Train the model.
    model.data_source = dataset_file
    model.submit(upload_data_source=True)
    print("Model training started for " + table)
    
    return model


def generate_data(rdb_config:dict, table:str, model):
    
    record_handler = model.create_record_handler_obj()
    
    # Get the dataset_file_path
    dataset_file = rdb_config["table_files"][table]

    # Submit the generation job
    record_handler.submit(
        action = "transform",
        data_source = dataset_file,
        upload_data_source = True
        )
    
    print("Generation started for " + table)
    
    return record_handler    
 
    
def transform_tables(rdb_config:dict, project, transform_policies:dict):

    # model_progress will hold the status of each model during training and generation
    model_progress = {}

    # transformed_tables will hold the final transformed tables
    transformed_tables = {}

    for table in rdb_config["table_files"]:
        if transform_policies[table] is not None:
            model = create_model(rdb_config, table, project, transform_policies)
            model_progress[table] = {
                         "model": model,
                         "model_status": "pending",
                         "record_handler": "",
                         "record_handler_status": "",
                         } 
        # If there is no transform on a table, copy it over as is
        else:
            transformed_tables[table] = rdb_config["table_data"][table]
        
        
    # Monitor model progression

    more_to_do = True
    while more_to_do:
    
        # Check status of training
        more_to_do = False
        for table in model_progress:
            status = model_progress[table]["model_status"]
            if (status == 'created') or (status == 'pending') or (status == "active"):
                more_to_do = True
                model = model_progress[table]["model"]
                model._poll_job_endpoint()
                status = model.__dict__['_data']['model']['status']
            
                # If status is now complete, submit the generation job
                if status == 'completed':
                    print("Training completed for " + table)
                    rh = generate_data(rdb_config, table, model)
                    model_progress[table]["record_handler"] = rh
                    model_progress[table]["record_handler_status"] = "pending"
                    
                model_progress[table]["model_status"] = status
                
            # If training status was already complete, check status of the generation job
            elif status == 'completed':
            
                status = model_progress[table]["record_handler_status"]
                if (status == 'created') or (status == 'pending') or (status == 'active'):
            
                    rh = model_progress[table]["record_handler"]
                    rh._poll_job_endpoint()
                    status = rh.__dict__['_data']['handler']['status']
                
                    # If generation is now complete, get the synthetic data
                    if status == 'completed':
                        transform_df = pd.read_csv(rh.get_artifact_link("data"), compression='gzip')
                        transformed_tables[table] = transform_df
                        model_progress[table]["record_handler_status"] = status
                        print("Generation completed for " + table)
                    
                    elif status != 'error':
                        more_to_do = True
                
                    else:
                        print("\nGeneration for " + table + " ended in error")
                        more_to_do = False
                    
                model_progress[table]["record_handler_status"] = status
                
            elif status == 'error':
                print("\nTraining for " + table + " ended in error")
                model_progress[table]["model_status"] = status
                more_to_do = False

    if status != 'error':
        print("\nModel training and initial generation all complete!")
        return transformed_tables, False
    else:
        return transformed_tables, True


def transform_keys(transformed_tables:dict, rdb_config:dict):
    
    primary_keys_processed = []
    
    # First process the primary/foreign key relationships
    
    for key_set in rdb_config["relationships"]:
    
        # Get array of unique values from each table, can use dfs in transformed_tables   
        first = True
        field_values = set()
        for table_field_pair in key_set:
            table, field = table_field_pair
            # The first pair is a primary key
            if first:
                primary_keys_processed.append(table)
                first = False
            field_values = field_values.union(set(rdb_config["table_data"][table][field]))
        
        # Train a label encoder
        field_values_list = list(set(field_values))
        le = preprocessing.LabelEncoder()
        le.fit(field_values_list)
    
        # Run the label encoder on dfs in transformed_tables
        for table_field_pair in key_set:
            table, field = table_field_pair
            transformed_tables[table][field] = le.transform(rdb_config["table_data"][table][field]) 

    # Process the remaining primary keys

    for table in rdb_config["primary_keys"]:
        if table not in primary_keys_processed:
            le = preprocessing.LabelEncoder()
            key_field = rdb_config["primary_keys"][table]
            le.fit(rdb_config["table_data"][table][key_field])
            transformed_tables[table][key_field] = le.transform(rdb_config["table_data"][table][key_field]) 
            
    return transformed_tables

            