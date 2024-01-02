import subprocess
import os
import glob
import duckdb
import shutil
import logging
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from tqdm import tqdm

# Configure logging to output to console
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_state(state, num_patients, data_dir, db_path):
    command = f"./run_synthea -p {num_patients} {state}"
    try:
        # Run the Synthea command
        result = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        logging.info(f"Output for {state}:\n{result.stdout}")

        # Connect to DuckDB database
        con = duckdb.connect(database=os.path.join(db_path, 'syn_dw.db'), read_only=False)

        # Create raw schema if it doesn't exist
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")

        # Load ndjson files produced by Synthea into DuckDB
        for file in glob.glob(f"{data_dir}/**/*.ndjson", recursive=True):
            table_name = os.path.splitext(os.path.basename(file))[0]
            con.execute(f"CREATE TABLE IF NOT EXISTS raw.{table_name} (data STRING)")
            
            with open(file, 'r') as f:
                for line in f:
                    # Insert each line as a new row in the table
                    con.execute(f"INSERT INTO raw.{table_name} (data) VALUES (?)", (line,))
            
            # Delete the file after it has been loaded into DuckDB
            os.remove(file)

        con.close()

    except subprocess.CalledProcessError as e:
        logging.error(f"Error running Synthea for {state}:\n{e.stderr}")

    except subprocess.CalledProcessError as e:
        logging.error(f"Error running Synthea for {state}:\n{e.stderr}")


def process_reference_tables(directory, db_path):
    directory = '/workspaces/synthea_dw/omop/seeds'
    all_files = os.listdir(directory)
    csv_files = [f for f in all_files if f.endswith('.csv')]

    # Connect to DuckDB
    con = duckdb.connect(db_path)

    for file in csv_files:
        file_path = os.path.join(directory, file)
        try:
            df = pd.read_csv(file_path, delimiter='\t', low_memory=False)
            dataframe_name = (os.path.splitext(file)[0]).lower()

            # Insert DataFrame into DuckDB under the 'reference' schema
            table_name = f"reference.{dataframe_name}"
            con.execute(f"CREATE SCHEMA IF NOT EXISTS reference")
            con.register(f"{dataframe_name}_df", df)
            con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM {dataframe_name}_df")
            logging.info(f"Successfully processed and loaded {file} into {table_name}")

        except pd.errors.ParserError as e:
            logging.error(f"Error reading file '{file}': {e}")

    con.close()
    logging.info("All reference tables processed and loaded into DuckDB.")


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
              "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York", 
              "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota", 
              "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming"]

    # Use ThreadPoolExecutor to parallelize the process
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_state = {executor.submit(process_state, state, num_patients, data_dir, db_path): state for state in states}

    shutil.rmtree(data_dir)
    logging.info("Data processing complete and temporary data directory removed.")

if __name__ == "__main__":
    
    directory = '/workspaces/synthea_dw/omop/seeds'
    db_path = '/workspaces/synthea_dw/syn_dw.db'

    # Ask user for number of patients to create
    num_patients = input("Enter the number of patients to create: ")

    # Ensure input is an integer
    try:
        num_patients = int(num_patients)
        logging.info(f"Starting Synthea simulation for {num_patients} patient(s).")
        run_synthea(num_patients)
        logging.info("Now processing reference tables.")
        process_reference_tables(directory, db_path)
    except ValueError:
        logging.error("Please enter a valid integer for the number of patients.")
        exit()