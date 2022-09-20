import random
import pandas as pd

from gretel_client import projects
from gretel_client import create_project
from gretel_client.config import RunnerMode


def create_model(table:str, project, training_configs:dict, training_data:dict):
    
    model = project.create_model_obj(model_config=training_configs[table])
    model.data_source = training_data[table]
    model.submit(upload_data_source=True)
    
    print("Model training started for " + table)
    
    return model


def generate_data(table:str, model, training_data:dict, synth_record_counts:dict):
    
    rh = model.create_record_handler_obj(training_data[table], params={"num_records": synth_record_counts[table]})
    rh.submit_cloud()
    
    print("Generation started for " + table)
    
    return rh 


def prepare_training_data(rdb_config:dict):
    
    # Remove all primary and foreign key fields from the training data

    # Start by gathering the columns for each table
    
    table_fields = {}
    table_fields_use = {}
    for table in rdb_config["table_data"]:
        table_fields[table] = list(rdb_config["table_data"][table].columns)
        table_fields_use[table] = list(table_fields[table])

    # Now, loop through the primary/foreign key relations and gather those columns
    
    primary_keys_processed = []
    for key_set in rdb_config["relationships"]:
        first = True
        for table_field_pair in key_set:
            table, field = table_field_pair
            if first:
                primary_keys_processed.append(table)
            table_fields_use[table].remove(field)
        
    # Gather the remaining primary keys
    
    for table in rdb_config["primary_keys"]:
        if table not in primary_keys_processed:
            key_field = primary_keys[table]
            table_fields_use[table].remove(field)
 
    # Remove the key fields from the training data
    
    training_data = {}
    for table in rdb_config["table_data"]:
        table_train = rdb_config["table_data"][table].filter(table_fields_use[table])
        filename = "./" + table + ".csv"
        table_train.to_csv(filename, index=False, header=True)
        training_data[table] = filename
        
    return training_data

def synthesize_keys(synthetic_tables:dict, rdb_config:dict,):
    
    # Reompute the number of records needed for each table
    
    synth_record_counts = {}
    for table in synthetic_tables:
        df = synthetic_tables[table]
        synth_record_counts[table] = len(df)
                
    # Synthesize primary keys by assigning a new unique int
    
    synth_primary_keys = {}
    for table in rdb_config["primary_keys"]:
        key = rdb_config["primary_keys"][table]
        df = synthetic_tables[table]
        synth_size = synth_record_counts[table]
        new_key = [i for i in range(synth_size)]
        synth_primary_keys[table] = new_key
        df[key] = new_key  
        synthetic_tables[table] = df
        
    # Synthesize foreign keys   

    synth_foreign_keys = {}
    for table in rdb_config["table_data"]:
        synth_foreign_keys[table] = {}

    for key_set in rdb_config["relationships"]:
        # The first table/field pair is the primary key
        first = True
        for table_field_pair in key_set:
            table, field = table_field_pair
            if first:
                primary_key_values = synth_primary_keys[table]
                first = False
            else:
                # Find the average number of records with the same foreign key value 
                synth_size = synth_record_counts[table]
                avg_cnt_key = int(synth_size / len(primary_key_values))
                key_values = []
                key_cnt = 0
                # Now recreate the foreign key values using the primary key values while
                # preserving the avg number of records with the same foreign key value
                for i in range(len(primary_key_values)):
                    for j in range(avg_cnt_key):
                        key_values.append(i)
                        key_cnt += 1
                i = 0
                while key_cnt < synth_size:
                    key_values.append(i)
                    key_cnt += 1
                    i += 1
                random.shuffle(key_values)
                synth_foreign_keys[table][field] = key_values
            
    for table in synth_foreign_keys:
        df = synthetic_tables[table]
        table_len = len(df)
        for field in synth_foreign_keys[table]:
            key = synth_foreign_keys[table][field]
            # Sanity check the new synthetic df and the foreign key match up in size
            key_use = key[0:table_len]
            df[field] = key_use
        synthetic_tables[table] = df
        
    return synthetic_tables

      
def synthesize_rdb(rdb_config:dict, project, training_configs:dict):

    # model_progress will hold the status of each model during training and generation
    model_progress = {}

    # transformed_tables will hold the final transformed tables
    synthetic_tables = {}
 
    # Compute the number of records needed for each table
    synth_record_counts = {}
    for table in rdb_config["table_data"]:
        df = rdb_config["table_data"][table]
        train_size = len(df)
        if table in rdb_config["tables_to_not_synthesize"]:
            synth_size = train_size
        else:
            synth_size = train_size * rdb_config["synth_record_size_ratio"]
        synth_record_counts[table] = synth_size
            
    # Prepare training data
    training_data = prepare_training_data(rdb_config)

    # Create a new project
    project = create_project(display_name="rdb_synthetics6")

    # Submit all models
    for table in rdb_config["table_files"]:
        if table not in rdb_config["tables_to_not_synthesize"]:
            model = create_model(table, project, training_configs, training_data)
            model_progress[table] = {
                         "model": model,
                         "model_status": "pending",
                         "record_handler": "",
                         "record_handler_status": "",
                         }        
        
    # Monitor model progression

    more_to_do = True
    no_errors = True
    while more_to_do and no_errors:
    
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
                    report = model.peek_report()
                    sqs = report['synthetic_data_quality_score']['score']
                    print_string = "Training completed for " + table + " with SQS score " + str(sqs)
                    if sqs >= 80:
                        print(print_string + " (Excellent)")
                    elif sqs >= 60:
                        print(print_string + " (Good)")
                    elif sqs >= 40:
                        print(print_string + " (Moderate)")
                    elif sqs >= 20:
                        print(print_string + " (Poor)")
                    else:
                        print(print_string + " (Very Poor)")
                    rh = generate_data(table, model, training_data, synth_record_counts)
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
                        df = pd.read_csv(rh.get_artifact_link("data"), compression='gzip')
                        synthetic_tables[table] = df
                        model_progress[table]["record_handler_status"] = status
                        print("Generation completed for " + table)
                    
                    elif status != 'error':
                        more_to_do = True
                
                    else:
                        print("\nGeneration for " + table + " ended in error")
                        no_errors = False
                    
                model_progress[table]["record_handler_status"] = status
                
            elif status == 'error':
                print("\nTraining for " + table + " ended in error")
                model_progress[table]["model_status"] = status
                no_errors = False
                
    # If table is in the list of those to not synthesize, move the train df to synth df list
    
    for table in rdb_config["tables_to_not_synthesize"]:
        filename = training_data[table]
        df = pd.read_csv(filename)
        synthetic_tables[table] = df
        
    # Now transfrom the primary and foreign keys
    
    if no_errors:
        print("\nModel training and initial generation all complete!")
        synthetic_tables = synthesize_keys(synthetic_tables, rdb_config)

    return synthetic_tables, model_progress
