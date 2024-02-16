import subprocess
import os
import glob
import duckdb
import shutil
import logging
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from tqdm import tqdm
import shutil
import gzip
import requests

# Configure logging to output to console
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_state(state, num_patients, data_dir, db_path):
    localConfigFilePath = '/workspaces/synthea_dw/synthea.properties'
    
    command = f"./run_synthea -c {localConfigFilePath} -p {num_patients} {state}"
    try:
        # Run the Synthea command
        result = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        logging.info(f"Output for {state}:\n{result.stdout}")

        # Connect to DuckDB database
        con = duckdb.connect(database=os.path.join(db_path, 'synthea.db'), read_only=False)

        # Create raw schema if it doesn't exist
        con.execute("CREATE SCHEMA IF NOT EXISTS json")

        # Load ndjson files produced by Synthea into DuckDB
        for file in glob.glob(f"{data_dir}/**/*.ndjson", recursive=True):
            table_name = os.path.splitext(os.path.basename(file))[0]

            # Use read_json_auto to load the NDJSON file into the table
            con.execute(f"CREATE TABLE IF NOT EXISTS json.{table_name} AS SELECT * FROM read_ndjson_auto('{file}', auto_detect=true, records='true', sample_size=-1)")
            
            # Delete the file after it has been loaded into DuckDB
            os.remove(file)

        con.close()

    except subprocess.CalledProcessError as e:
        logging.error(f"Error running Synthea for {state}:\n{e.stderr}")


def process_vocabulary_tables(directory, db_path):
    directory = '/workspaces/synthea_dw/vocabulary'
    all_files = os.listdir(directory)
    csv_files = [f for f in all_files if f.endswith('.csv')]

    # Connect to DuckDB
    con = duckdb.connect(db_path)

    for file in csv_files:
        file_path = os.path.join(directory, file)
        try:
            df = pd.read_csv(file_path, delimiter='\t', low_memory=False)
            dataframe_name = (os.path.splitext(file)[0]).lower()

            # Insert DataFrame into DuckDB under the 'vocabulary' schema
            table_name = f"vocabulary.{dataframe_name}"
            con.execute(f"CREATE SCHEMA IF NOT EXISTS vocabulary")
            con.register(f"{dataframe_name}_df", df)
            con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM {dataframe_name}_df")
            logging.info(f"Successfully processed and loaded {file} into {table_name}")

        except pd.errors.ParserError as e:
            logging.error(f"Error reading file '{file}': {e}")

    con.close()
    logging.info("All vocabulary tables processed and loaded into DuckDB.")


def process_reference_tables(directory, db_path):
    directory = '/workspaces/synthea_dw/reference'
    all_files = os.listdir(directory)
    parquet_files = [f for f in all_files if f.endswith('.parquet')]

    # Connect to DuckDB
    con = duckdb.connect(db_path)

    for file in parquet_files:
        file_path = os.path.join(directory, file)
        try:
            df = pd.read_parquet(file_path)
            dataframe_name = (os.path.splitext(file)[0]).lower()

            # Insert DataFrame into DuckDB under the 'reference' schema
            table_name = f"reference.{dataframe_name}"
            con.execute(f"CREATE SCHEMA IF NOT EXISTS reference")
            con.register(f"{dataframe_name}_df", df)
            con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM {dataframe_name}_df")
            logging.info(f"Successfully processed and loaded {file} into {table_name}")

        except Exception as e:
            logging.error(f"Error reading file '{file}': {e}")

    con.close()
    logging.info("All reference tables processed and loaded into DuckDB.")


def process_terminology_tables(db_path, terminology_base_directory):
    # Connect to DuckDB
    con = duckdb.connect(db_path)

    # Iterate over the subdirectories within the terminology base directory
    for subdirectory_name in os.listdir(terminology_base_directory):
        subdirectory_path = os.path.join(terminology_base_directory, subdirectory_name)

        # Check if the path is a directory
        if os.path.isdir(subdirectory_path):
            # Create a schema for the subdirectory
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {subdirectory_name}")

            # Process Parquet files in the subdirectory
            for filename in os.listdir(subdirectory_path):
                if filename.endswith('.parquet'):
                    parquet_file_path = os.path.join(subdirectory_path, filename)
                    table_name = f"{subdirectory_name}.{os.path.splitext(filename)[0]}"

                    try:
                        # Load Parquet file into the DuckDB table
                        df = pd.read_parquet(parquet_file_path)
                        con.register(f"{os.path.splitext(filename)[0]}_df", df)
                        con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM {os.path.splitext(filename)[0]}_df")
                        logging.info(f"Successfully processed and loaded {filename} into {table_name} in the {subdirectory_name} schema")
                    except Exception as e:
                        logging.error(f"Error processing file '{filename}' in the {subdirectory_name} schema: {e}")

    con.close()
    logging.info("All terminology tables processed and loaded into DuckDB.")


def run_synthea(num_patients):
    synthea_path = '/workspaces/synthea_dw/synthea'
    data_dir = '/workspaces/synthea_dw/data'
    db_path = '/workspaces/synthea_dw'

    # Create data directory if it doesn't exist
    os.makedirs(data_dir, exist_ok=True)
    os.chdir(synthea_path)
    
    # List of states
    states = ["Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", 
              "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", 
              "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "'New Hampshire'", "'New Jersey'", "'New Mexico'", "'New York'", 
              "'North Carolina'", "'North Dakota'", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "'Rhode Island'", "'South Carolina'", "'South Dakota'", 
              "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington", "'West Virginia'", "Wisconsin", "Wyoming"]

    # Use ThreadPoolExecutor to parallelize the process
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_state = {executor.submit(process_state, state, num_patients, data_dir, db_path): state for state in states}

    shutil.rmtree(data_dir)
    logging.info("Data processing complete and temporary data directory removed.")


if __name__ == "__main__":
    reference_directory = '/workspaces/synthea_dw/reference'
    vocabulary_directory = '/workspaces/synthea_dw/vocabulary'
    db_path = '/workspaces/synthea_dw/synthea.db'
    terminology_base_directory='/workspaces/synthea_dw/terminology'
    
    # Ask user for number of patients to create
    num_patients = input("Enter the number of patients to create: ")

    # Ensure input is an integer
    try:
        num_patients = int(num_patients)
        logging.info(f"Starting Synthea simulation for {num_patients} patient(s).")
        run_synthea(num_patients)
        
        logging.info("Now processing vocabulary tables.")
        process_vocabulary_tables(vocabulary_directory, db_path)
        
        logging.info("Now processing reference tables.")
        process_reference_tables(reference_directory, db_path)
        
        logging.info("Now processing terminology tables.")
        process_terminology_tables(db_path, terminology_base_directory)
        
    except ValueError:
        logging.error("Please enter a valid integer for the number of patients.")
        exit()