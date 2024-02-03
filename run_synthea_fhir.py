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
    command = f"./run_synthea -p {num_patients} {state}"
    try:
        # Run the Synthea command
        result = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        logging.info(f"Output for {state}:\n{result.stdout}")

        # Connect to DuckDB database
        con = duckdb.connect(database=os.path.join(db_path, 'synthea_fhir.db'), read_only=False)

        # Create raw schema if it doesn't exist
        con.execute("CREATE SCHEMA IF NOT EXISTS json")

        # Load ndjson files produced by Synthea into DuckDB
        for file in glob.glob(f"{data_dir}/**/*.ndjson", recursive=True):
            table_name = os.path.splitext(os.path.basename(file))[0]

            # Use read_json_auto to load the NDJSON file into the table
            con.execute(f"CREATE TABLE json.{table_name} AS SELECT * FROM read_ndjson_auto('{file}', auto_detect=true, records='true', sample_size=-1)")

            # Delete the file after it has been loaded into DuckDB
            os.remove(file)

        con.close()

    except subprocess.CalledProcessError as e:
        logging.error(f"Error running Synthea for {state}:\n{e.stderr}")

    except subprocess.CalledProcessError as e:
        logging.error(f"Error running Synthea for {state}:\n{e.stderr}")


def process_vocabulary_tables(directory, db_path):
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


def run_synthea(num_patients):
    synthea_path = '/workspaces/synthea_dw/synthea'
    data_dir = '/workspaces/synthea_dw/data'
    db_path = '/workspaces/synthea_dw'

    # Create data directory if it doesn't exist
    os.makedirs(data_dir, exist_ok=True)
    os.chdir(synthea_path)
    
    # List of states
    states = ["Alabama", "Wisconsin", "Wyoming", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana"]

    # Use ThreadPoolExecutor to parallelize the process
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_state = {executor.submit(process_state, state, num_patients, data_dir, db_path): state for state in states}

    shutil.rmtree(data_dir)
    logging.info("Data processing complete and temporary data directory removed.")


def download_and_decompress(url, local_file_path):
    try:
        # Download the file from the URL
        response = requests.get(url, stream=True)
        response.raise_for_status()

        # Save the .gz file locally
        with open(local_file_path + '.gz', 'wb') as f:
            f.write(response.content)

        # Decompress gzip file
        with gzip.open(local_file_path + '.gz', 'rb') as f_in:
            with open(local_file_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        # Remove the compressed file
        os.remove(local_file_path + '.gz')
        logging.info(f"File from {url} downloaded and decompressed successfully.")
    except Exception as e:
        logging.error(f"Error downloading from {url}: {e}")


def process_terminology(schema_name, table_definitions, base_url, directory, db_path):
    # Create the directory for file download
    os.makedirs(directory, exist_ok=True)

    # Connect to DuckDB
    con = duckdb.connect(db_path)
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    for table_def in table_definitions:
        table_name = f"{schema_name}.{table_def['name']}"

        file_url = base_url + table_def['s3_path'] + '_0_0_0.csv.gz'
        local_file_path = os.path.join(directory, table_def['name'] + '.csv')
        download_and_decompress(file_url, local_file_path)

        try:
            # Define columns and their data types for read_csv
            column_defs = {}
            for column_def in table_def['columns'].split(','):
                col_name, col_type = column_def.strip().split(maxsplit=1)
                column_defs[col_name] = col_type

            # Use DuckDB's read_csv to directly create and load the data into the table
            con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM read_csv('{local_file_path}', COLUMNS={column_defs}, SAMPLE_SIZE=-1, AUTO_DETECT=TRUE, NULLSTR='\\N')")
            logging.info(f"Successfully processed and loaded {table_def['name']}.csv into {table_name}")
        except Exception as e:
            logging.error(f"Error loading file '{table_def['name']}.csv' into DuckDB: {e}")

    con.close()

    # Remove the directory after processing
    shutil.rmtree(directory)
    logging.info(f"All tables processed and loaded into DuckDB under the {schema_name} schema, and directory cleaned up.")

terminology_tables = [
    {
        'name': 'admit_source',
        'columns': 'admit_source_code VARCHAR, admit_source_description VARCHAR, newborn_description VARCHAR',
        's3_path': 'terminology/admit_source.csv'
    },
    {
        'name': 'admit_type',
        'columns': 'admit_type_code VARCHAR, admit_type_description VARCHAR',
        's3_path': 'terminology/admit_type.csv'
    },
    {
        'name': 'ansi_fips_state',
        'columns': 'ansi_fips_state_code VARCHAR, ansi_fips_state_abbreviation VARCHAR, ansi_fips_state_name VARCHAR',
        's3_path': 'terminology/ansi_fips_state.csv'
    },
    {
        'name': 'apr_drg',
        'columns': 'apr_drg_code VARCHAR, severity VARCHAR, apr_drg_description VARCHAR',
        's3_path': 'terminology/apr_drg.csv'
    },
    {
        'name': 'bill_type',
        'columns': 'bill_type_code VARCHAR, bill_type_description VARCHAR, deprecated INTEGER, deprecated_date DATE',
        's3_path': 'terminology/bill_type.csv'
    },
    {
        'name': 'calendar',
        'columns': 'full_date DATE, "year" INTEGER, "month" INTEGER, "day" INTEGER, month_name VARCHAR, day_of_week_number INTEGER, day_of_week_name VARCHAR, week_of_year INTEGER, day_of_year INTEGER, year_month VARCHAR, first_day_of_month DATE, last_day_of_month DATE',
        's3_path': 'terminology/calendar.csv'
    },
    {
        'name': 'claim_type',
        'columns': 'claim_type VARCHAR',
        's3_path': 'terminology/claim_type.csv'
    },
    {
        'name': 'code_type',
        'columns': 'code_type VARCHAR',
        's3_path': 'terminology/code_type.csv'
    },
    {
        'name': 'discharge_disposition',
        'columns': 'discharge_disposition_code VARCHAR, discharge_disposition_description VARCHAR',
        's3_path': 'terminology/discharge_disposition.csv'
    },
    {
        'name': 'encounter_type',
        'columns': 'encounter_type VARCHAR',
        's3_path': 'terminology/encounter_type.csv'
    },
    {
        'name': 'ethnicity',
        'columns': 'code VARCHAR, description VARCHAR',
        's3_path': 'terminology/ethnicity.csv'
    },
    {
        'name': 'fips_county',
        'columns': 'fips_code VARCHAR, county VARCHAR, state VARCHAR',
        's3_path': 'terminology/fips_county.csv'
    },
    {
        'name': 'gender',
        'columns': 'gender VARCHAR',
        's3_path': 'terminology/gender.csv'
    },
    {
        'name': 'hcpcs_level_2',
        'columns': 'hcpcs VARCHAR, seqnum VARCHAR, recid VARCHAR, long_description VARCHAR, short_description VARCHAR',
        's3_path': 'terminology/hcpcs_level_2.csv'
    },
    {
        'name': 'icd_10_cm',
        'columns': 'icd_10_cm VARCHAR, description VARCHAR',
        's3_path': 'terminology/icd_10_cm.csv'
    },
    {
        'name': 'icd_10_pcs',
        'columns': 'icd_10_pcs VARCHAR, description VARCHAR',
        's3_path': 'terminology/icd_10_pcs.csv'
    },
    {
        'name': 'icd_9_cm',
        'columns': 'icd_9_cm VARCHAR, long_description VARCHAR, short_description VARCHAR',
        's3_path': 'terminology/icd_9_cm.csv'
    },
    {
        'name': 'icd_9_pcs',
        'columns': 'icd_9_pcs VARCHAR, long_description VARCHAR, short_description VARCHAR',
        's3_path': 'terminology/icd_9_pcs.csv'
    },
    {
        'name': 'loinc',
        'columns': 'loinc VARCHAR, short_name VARCHAR, long_common_name VARCHAR, component VARCHAR, property VARCHAR, time_aspect VARCHAR, system VARCHAR, scale_type VARCHAR, method_type VARCHAR, class_code VARCHAR, class_description VARCHAR, class_type_code VARCHAR, class_type_description VARCHAR, external_copyright_notice VARCHAR, status VARCHAR, version_first_released VARCHAR, version_last_changed VARCHAR',
        's3_path': 'value-sets/loinc.csv'
    },
    {
        'name': 'loinc_deprecated_mapping',
        'columns': 'loinc VARCHAR, map_to VARCHAR, comment VARCHAR, final_map_to VARCHAR, all_comments VARCHAR, depth VARCHAR',
        's3_path': 'terminology/loinc_deprecated_mapping.csv'
    },
    {
        'name': 'mdc',
        'columns': 'mdc_code VARCHAR, mdc_description VARCHAR',
        's3_path': 'terminology/mdc.csv'
    },
    {
        'name': 'medicare_dual_eligibility',
        'columns': 'dual_status_code VARCHAR, dual_status_description VARCHAR',
        's3_path': 'terminology/medicare_dual_eligibility.csv'
    },
    {
        'name': 'medicare_orec',
        'columns': 'original_reason_entitlement_code VARCHAR, original_reason_entitlement_description VARCHAR',
        's3_path': 'terminology/medicare_orec.csv'
    },
    {
        'name': 'medicare_status',
        'columns': 'medicare_status_code VARCHAR, medicare_status_description VARCHAR',
        's3_path': 'terminology/medicare_status.csv'
    },
    {
        'name': 'ms_drg',
        'columns': 'ms_drg_code VARCHAR, mdc_code VARCHAR, medical_surgical VARCHAR, ms_drg_description VARCHAR, deprecated INTEGER, deprecated_date DATE',
        's3_path': 'terminology/ms_drg.csv'
    },
    {
        'name': 'other_provider_taxonomy',
        'columns': 'npi VARCHAR, taxonomy_code VARCHAR, medicare_specialty_code VARCHAR, description VARCHAR, primary_flag INTEGER',
        's3_path': 'terminology/other_provider_taxonomy.csv'
    },
    {
        'name': 'payer_type',
        'columns': 'payer_type VARCHAR',
        's3_path': 'terminology/payer_type.csv'
    },
    {
        'name': 'place_of_service',
        'columns': 'place_of_service_code VARCHAR, place_of_service_description VARCHAR',
        's3_path': 'terminology/place_of_service.csv'
    },
    {
        'name': 'present_on_admission',
        'columns': 'present_on_admit_code VARCHAR, present_on_admit_description VARCHAR',
        's3_path': 'terminology/present_on_admission.csv'
    },
    {
        'name': 'provider',
        'columns': 'npi VARCHAR, entity_type_code VARCHAR, entity_type_description VARCHAR, primary_taxonomy_code VARCHAR, primary_specialty_description VARCHAR, provider_name VARCHAR, parent_organization_name VARCHAR, practice_address_line_1 VARCHAR, practice_address_line_2 VARCHAR, practice_city VARCHAR, practice_state VARCHAR, practice_zip_code VARCHAR, last_updated DATE, deactivation_date DATE, deactivation_flag VARCHAR',
        's3_path': 'terminology/provider.csv'
    },
    {
        'name': 'race',
        'columns': 'code VARCHAR, description VARCHAR',
        's3_path': 'terminology/race.csv'
    },
    {
        'name': 'revenue_center',
        'columns': 'revenue_center_code VARCHAR, revenue_center_description VARCHAR',
        's3_path': 'terminology/revenue_center.csv'
    },
    {
        'name': 'ssa_fips_state',
        'columns': 'ssa_fips_state_code VARCHAR, ssa_fips_state_name VARCHAR',
        's3_path': 'terminology/ssa_fips_state.csv'
    }
]

value_set_tables = [
    {
        'name': 'acute_diagnosis_ccs',
        'columns': 'ccs_diagnosis_category VARCHAR, description VARCHAR',
        's3_path': 'value-sets/acute_diagnosis_ccs.csv'
    },
    {
        'name': 'acute_diagnosis_icd_10_cm',
        'columns': 'icd_10_cm VARCHAR, description VARCHAR',
        's3_path': 'value-sets/acute_diagnosis_icd_10_cm.csv'
    },
    {
        'name': 'always_planned_ccs_diagnosis_category',
        'columns': 'ccs_diagnosis_category VARCHAR, description VARCHAR',
        's3_path': 'value-sets/always_planned_ccs_diagnosis_category.csv'
    },
    {
        'name': 'always_planned_ccs_procedure_category',
        'columns': 'ccs_procedure_category VARCHAR, description VARCHAR',
        's3_path': 'value-sets/always_planned_ccs_procedure_category.csv'
    },
    {
        'name': 'cms_chronic_conditions_hierarchy',
        'columns': 'condition_id INTEGER, condition VARCHAR, condition_column_name VARCHAR, chronic_condition_type VARCHAR, condition_category VARCHAR, additional_logic VARCHAR, claims_qualification VARCHAR, inclusion_type VARCHAR, code_system VARCHAR, code VARCHAR',
        's3_path': 'value-sets/cms_chronic_conditions_hierarchy.csv'
    },
    {
        'name': 'exclusion_ccs_diagnosis_category',
        'columns': 'ccs_diagnosis_category VARCHAR, description VARCHAR, exclusion_category VARCHAR',
        's3_path': 'value-sets/exclusion_ccs_diagnosis_category.csv'
    },
    {
        'name': 'icd_10_cm_to_ccs',
        'columns': 'icd_10_cm VARCHAR, description VARCHAR, ccs_diagnosis_category VARCHAR, ccs_description VARCHAR',
        's3_path': 'value-sets/icd_10_cm_to_ccs.csv'
    },
    {
        'name': 'icd_10_pcs_to_ccs',
        'columns': 'icd_10_pcs VARCHAR, description VARCHAR, ccs_procedure_category VARCHAR, ccs_description VARCHAR',
        's3_path': 'value-sets/icd_10_pcs_to_ccs.csv'
    },
    {
        'name': 'potentially_planned_ccs_procedure_category',
        'columns': 'ccs_procedure_category VARCHAR, description VARCHAR',
        's3_path': 'value-sets/potentially_planned_ccs_procedure_category.csv'
    },
    {
        'name': 'potentially_planned_icd_10_pcs',
        'columns': 'icd_10_pcs VARCHAR, description VARCHAR',
        's3_path': 'value-sets/potentially_planned_icd_10_pcs.csv'
    },
    {
        'name': 'service_category',
        'columns': 'service_category_1 VARCHAR, service_category_2 VARCHAR, claim_type VARCHAR, hcpcs_code VARCHAR, bill_type_code_first_2_digits VARCHAR, revenue_center_code VARCHAR, valid_drg_flag VARCHAR, place_of_service_code VARCHAR',
        's3_path': 'value-sets/service_category.csv'
    },
    {
        'name': 'specialty_cohort',
        'columns': 'ccs VARCHAR, description VARCHAR, specialty_cohort VARCHAR, procedure_or_diagnosis VARCHAR',
        's3_path': 'value-sets/specialty_cohort.csv'
    },
    {
        'name': 'surgery_gynecology_cohort',
        'columns': 'icd_10_pcs VARCHAR, description VARCHAR, ccs_code_and_description VARCHAR, specialty_cohort VARCHAR',
        's3_path': 'value-sets/surgery_gynecology_cohort.csv'
    },
    {
        'name': 'tuva_chronic_conditions_hierarchy',
        'columns': 'condition_family VARCHAR, condition VARCHAR, icd_10_cm_code VARCHAR, icd_10_cm_description VARCHAR, condition_column_name VARCHAR',
        's3_path': 'value-sets/tuva_chronic_conditions_hierarchy.csv'
    }
]

if __name__ == "__main__":
    directory = '/workspaces/synthea_dw/omop/seeds'
    db_path = '/workspaces/synthea_dw/synthea_fhir.db'
    base_url = 'https://tuva-public-resources.s3.amazonaws.com/'
    
    # Ask user for number of patients to create
    num_patients = input("Enter the number of patients to create: ")

    # Ensure input is an integer
    try:
        num_patients = int(num_patients)
        logging.info(f"Starting Synthea simulation for {num_patients} patient(s).")
        run_synthea(num_patients)
        logging.info("Now processing vocabulary tables.")
        process_vocabulary_tables(directory, db_path)

        # Processing terminology and value_set tables
        logging.info("Now processing terminology tables.")
        process_terminology('terminology', terminology_tables, base_url, '/workspaces/synthea_dw/data/', db_path)
        logging.info("Now processing value_set tables.")
        process_terminology('value_sets', value_set_tables, base_url, '/workspaces/synthea_dw/data/', db_path)
    except ValueError:
        logging.error("Please enter a valid integer for the number of patients.")
        exit()