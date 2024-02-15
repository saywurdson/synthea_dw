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


def process_terminology_tables(schema_name, table_definitions, base_url, directory, db_path, parquet_base_directory=None):
    # Create the directory for file download
    os.makedirs(directory, exist_ok=True)

    # Connect to DuckDB
    con = duckdb.connect(db_path)
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    # Process CSVs with schema field
    for table_def in table_definitions:
        # Use the schema field from table definition, fallback to default schema_name if not provided
        table_schema = table_def.get('schema', schema_name)
        table_name = f"{table_schema}.{table_def['name']}"
        file_url = base_url + table_def['s3_path'] + '_0_0_0.csv.gz'
        local_file_path = os.path.join(directory, table_def['name'] + '.csv.gz')

        # Ensure the schema exists
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {table_schema}")

        download_and_decompress(file_url, local_file_path)

        try:
            column_defs = {}
            for column_def in table_def['columns'].split(','):
                col_name, col_type = column_def.strip().split(maxsplit=1)
                column_defs[col_name] = col_type

            con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM read_csv_auto('{local_file_path}', NULLSTR='\\N')")
            logging.info(f"Successfully processed and loaded {table_def['name']}.csv.gz into {table_name}")
        except Exception as e:
            logging.error(f"Error loading file '{table_def['name']}.csv.gz' into DuckDB: {e}")

    # Existing segment for processing Parquet files remains unchanged

    con.close()

    # Remove the directory after processing
    shutil.rmtree(directory)
    logging.info(f"All tables processed and loaded into DuckDB, and directory cleaned up.")

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
        'name': 'ndc',
        'columns': 'ndc VARCHAR, rxcui VARCHAR, rxnorm_description VARCHAR, fda_description VARCHAR',
        's3_path': 'terminology/ndc.csv'
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
        'name': 'rxnorm_to_atc',
        'columns': 'rxcui VARCHAR, rxnorm_description VARCHAR, atc_1_name VARCHAR, atc_2_name VARCHAR, atc_3_name VARCHAR, atc_4_name VARCHAR',
        's3_path': 'terminology/rxnorm_to_atc.csv'
    },
    {
        'name': 'ssa_fips_state',
        'columns': 'ssa_fips_state_code VARCHAR, ssa_fips_state_name VARCHAR',
        's3_path': 'terminology/ssa_fips_state.csv'
    },
    {
        'name': 'snomed_icd_10_map',
        'columns': 'id VARCHAR, effective_time VARCHAR, active INTEGER, module_id VARCHAR, ref_set_id VARCHAR, referenced_component_id VARCHAR, referenced_component_name VARCHAR, map_group INTEGER, map_priority INTEGER, map_rule VARCHAR, map_advice VARCHAR, map_target VARCHAR, map_target_name VARCHAR, correlation_id VARCHAR, map_category_id VARCHAR, map_category_name VARCHAR',
        's3_path': 'terminology/snomed_icd_10_map.csv'
    },
]

value_set_tables = [
    {
        'name': '_value_set_dxccsr_v2023_1_body_systems',
        'columns': 'body_system	VARCHAR, ccsr_parent_category VARCHAR, parent_category_description VARCHAR',
        'schema': 'ccsr',
        's3_path': 'value-sets/dxccsr_v2023_1_body_systems.csv'
    },
    {
        'name': '_value_set_dxccsr_v2023_1_cleaned_map',
        'columns': 'icd_10_cm_code VARCHAR, icd_10_cm_code_description VARCHAR, default_ccsr_category_ip VARCHAR, default_ccsr_category_description_ip VARCHAR, default_ccsr_category_op VARCHAR, default_ccsr_category_description_op VARCHAR, ccsr_category_1 VARCHAR, ccsr_category_1_description VARCHAR, ccsr_category_2 VARCHAR, ccsr_category_2_description VARCHAR, ccsr_category_3 VARCHAR, ccsr_category_3_description VARCHAR, ccsr_category_4 VARCHAR, ccsr_category_4_description VARCHAR, ccsr_category_5 VARCHAR, ccsr_category_5_description VARCHAR, ccsr_category_6 VARCHAR, ccsr_category_6_description VARCHAR',
        'schema': 'ccsr',
        's3_path': 'value-sets/dxccsr_v2023_1_cleaned_map.csv'
    },
    {
        'name': '_value_set_prccsr_v2023_1_cleaned_map',
        'columns': 'icd_10_pcs VARCHAR, icd_10_pcs_description VARCHAR, prccsr VARCHAR, prccsr_description VARCHAR, clinical_domain VARCHAR',
        'schema': 'ccsr',
        's3_path': 'value-sets/prccsr_v2023_1_cleaned_map.csv'
    },
    {
        'name': '_value_set_cms_chronic_conditions_hierarchy',
        'columns': 'condition_id INTEGER, condition VARCHAR, condition_column_name VARCHAR, chronic_condition_type VARCHAR, condition_category VARCHAR, additional_logic VARCHAR, claims_qualification VARCHAR, inclusion_type VARCHAR, code_system VARCHAR, code VARCHAR',
        'schema': 'chronic_conditions',
        's3_path': 'value-sets/cms_chronic_conditions_hierarchy.csv'
    },
    {
        'name': '_value_set_tuva_chronic_conditions_hierarchy',
        'columns': 'condition_family VARCHAR, condition VARCHAR, icd_10_cm_code VARCHAR, icd_10_cm_description VARCHAR, condition_column_name VARCHAR',
        'schema': 'chronic_conditions',
        's3_path': 'value-sets/tuva_chronic_conditions_hierarchy.csv'
    },
    {
        'name': '_value_set_adjustment_rates',
        'columns': 'model_version VARCHAR, payment_year VARCHAR, normalization_factor VARCHAR, ma_coding_pattern_adjustment VARCHAR',
        'schema': 'cms_hcc',
        's3_path': 'value-sets/adjustment_rates.csv'
    },
    {
        'name': '_value_set_cpt_hcpcs',
        'columns': 'payment_year VARCHAR, hcpcs_cpt_code VARCHAR, included_flag VARCHAR',
        'schema': 'cms_hcc',
        's3_path': 'value-sets/cpt_hcpcs.csv'
    },
    {
        'name': '_value_set_demographic_factors',
        'columns': 'model_version VARCHAR, factor_type VARCHAR, enrollment_status VARCHAR, plan_segment VARCHAR, gender VARCHAR, age_group VARCHAR, medicaid_status VARCHAR, dual_status VARCHAR, orec VARCHAR, institutional_status VARCHAR, coefficient VARCHAR',
        'schema': 'cms_hcc',
        's3_path': 'value-sets/test_catalog.csv'
    },
    {
        'name': '_value_set_disabled_interaction_factors',
        'columns': 'model_version VARCHAR, factor_type VARCHAR, enrollment_status VARCHAR, institutional_status VARCHAR, short_name VARCHAR, description VARCHAR, hcc_code VARCHAR, coefficient VARCHAR',
        'schema': 'cms_hcc',
        's3_path': 'value-sets/disabled_interaction_factors.csv'
    },
    {
        'name': '_value_set_disease_factors',
        'columns': 'model_version VARCHAR, factor_type VARCHAR, enrollment_status VARCHAR, medicaid_status VARCHAR, dual_status VARCHAR, orec VARCHAR, institutional_status VARCHAR, hcc_code VARCHAR, description VARCHAR, coefficient VARCHAR',
        'schema': 'cms_hcc',
        's3_path': 'value-sets/disease_factors.csv'
    },
    {
        'name': '_value_set_disease_hierarchy',
        'columns': 'model_version VARCHAR, hcc_code VARCHAR, description VARCHAR, hccs_to_exclude VARCHAR',
        'schema': 'cms_hcc',
        's3_path': 'value-sets/disease_hierarchy.csv'
    },
    {
        'name': '_value_set_disease_interaction_factors',
        'columns': 'model_version VARCHAR, factor_type VARCHAR, enrollment_status VARCHAR, medicaid_status VARCHAR, dual_status VARCHAR, orec VARCHAR, institutional_status VARCHAR, short_name VARCHAR, description VARCHAR, hcc_code_1 VARCHAR, hcc_code_2 VARCHAR, coefficient VARCHAR',
        'schema': 'cms_hcc',
        's3_path': 'value-sets/disease_interaction_factors.csv'
    },
    {
        'name': '_value_set_enrollment_interaction_factors',
        'columns': 'model_version VARCHAR, factor_type VARCHAR, gender VARCHAR, enrollment_status VARCHAR, medicaid_status VARCHAR, dual_status VARCHAR, institutional_status VARCHAR, description VARCHAR, coefficient VARCHAR',
        'schema': 'cms_hcc',
        's3_path': 'value-sets/enrollment_interaction_factors.csv'
    },
    {
        'name': '_value_set_icd_10_cm_mappings',
        'columns': 'payment_year VARCHAR, diagnosis_code VARCHAR, cms_hcc_v24 VARCHAR, cms_hcc_v24_flag VARCHAR',
        'schema': 'cms_hcc',
        's3_path': 'value-sets/icd_10_cm_mappings.csv'
    },
    {
        'name': '_value_set_payment_hcc_count_factors',
        'columns': 'model_version VARCHAR, factor_type VARCHAR, enrollment_status VARCHAR, medicaid_status VARCHAR, dual_status VARCHAR, orec VARCHAR, institutional_status VARCHAR, payment_hcc_count VARCHAR, description VARCHAR, coefficient VARCHAR',
        'schema': 'cms_hcc',
        's3_path': 'value-sets/payment_hcc_count_factors.csv'
    },
    {
        'name': '_value_set_test_catalog',
        'columns': 'source_table VARCHAR, test_category VARCHAR, test_name VARCHAR, test_field VARCHAR, claim_type VARCHAR, pipeline_test VARCHAR',
        'schema': 'data_quality',
        's3_path': 'value-sets/categories.csv'
    },
    {
        'name': '_value_set_categories',
        'columns': 'classification VARCHAR, classification_name VARCHAR, classification_order VARCHAR, classification_column VARCHAR',
        'schema': 'ed_classification',
        's3_path': 'value-sets/categories.csv'
    },
    {
        'name': '_value_set_johnston_icd9',
        'columns': 'icd9 VARCHAR, edcnnpa VARCHAR, edcnpa VARCHAR, epct VARCHAR, noner VARCHAR, injury VARCHAR, psych VARCHAR, alcohol VARCHAR, drug VARCHAR',
        'schema': 'ed_classification',
        's3_path': 'value-sets/johnston_icd9.csv'
    },
    {
        'name': '_value_set_johnston_icd10',
        'columns': 'icd10 VARCHAR, edcnnpa VARCHAR, edcnpa VARCHAR, epct VARCHAR, noner VARCHAR, injury VARCHAR, psych VARCHAR, alcohol VARCHAR, drug VARCHAR',
        'schema': 'ed_classification',
        's3_path': 'value-sets/johnston_icd10.csv'
    },
    {
        'name': '_value_set_hcc_descriptions',
        'columns': 'hcc_code VARCHAR, hcc_description VARCHAR',
        'schema': 'hcc_suspecting',
        's3_path': 'value-sets/codes.csv'
    },
    {
        'name': '_value_set_icd_10_cm_mappings',
        'columns': 'diagnosis_code VARCHAR, cms_hcc_esrd_v21 VARCHAR, cms_hcc_esrd_v24 VARCHAR, cms_hcc_v22 VARCHAR, cms_hcc_v24 VARCHAR, cms_hcc_v28 VARCHAR, rx_hcc_v05 VARCHAR, rx_hcc_v08 VARCHAR',
        'schema': 'hcc_suspecting',
        's3_path': 'value-sets/codes.csv'
    },
    {
        'name': '_value_set_codes',
        'columns': 'concept_name VARCHAR, concept_oid VARCHAR, code VARCHAR, code_system VARCHAR',
        'schema': 'quality_measures',
        's3_path': 'value-sets/codes.csv'
    },
    {
        'name': '_value_set_concepts',
        'columns': 'concept_name VARCHAR, concept_oid VARCHAR, measure_id VARCHAR, measure_name VARCHAR',
        'schema': 'quality_measures',
        's3_path': 'value-sets/concepts.csv'
    },
    {
        'name': '_value_set_measures',
        'columns': 'id VARCHAR, name VARCHAR, description VARCHAR, version VARCHAR, steward VARCHAR, compatible_measure VARCHAR',
        'schema': 'quality_measures',
        's3_path': 'value-sets/measures.csv'
    },
    {
        'name': '_value_set_value_sets',
        'columns': 'concept_name VARCHAR, concept_oid VARCHAR, code VARCHAR, code_system VARCHAR',
        'schema': 'quality_measures',
        's3_path': 'value-sets/value_sets.csv'
    },
    {
        'name': '_value_set_acute_diagnosis_ccs',
        'columns': 'ccs_diagnosis_category VARCHAR, description VARCHAR',
        'schema': 'readmissions',
        's3_path': 'value-sets/acute_diagnosis_ccs.csv'
    },
    {
        'name': '_value_set_acute_diagnosis_icd_10_cm',
        'columns': 'icd_10_cm VARCHAR, description VARCHAR',
        'schema': 'readmissions',
        's3_path': 'value-sets/acute_diagnosis_icd_10_cm.csv'
    },
    {
        'name': '_value_set_always_planned_ccs_diagnosis_category',
        'columns': 'ccs_diagnosis_category VARCHAR, description VARCHAR',
        'schema': 'readmissions',
        's3_path': 'value-sets/always_planned_ccs_diagnosis_category.csv'
    },
    {
        'name': '_value_set_always_planned_ccs_procedure_category',
        'columns': 'ccs_procedure_category VARCHAR, description VARCHAR',
        'schema': 'readmissions',
        's3_path': 'value-sets/always_planned_ccs_procedure_category.csv'
    },
    {
        'name': '_value_set_exclusion_ccs_diagnosis_category',
        'columns': 'ccs_diagnosis_category VARCHAR, description VARCHAR, exclusion_category VARCHAR',
        'schema': 'readmissions',
        's3_path': 'value-sets/exclusion_ccs_diagnosis_category.csv'
    },
    {
        'name': '_value_set_icd_10_cm_to_ccs',
        'columns': 'icd_10_cm VARCHAR, description VARCHAR, ccs_diagnosis_category VARCHAR, ccs_description VARCHAR',
        'schema': 'readmissions',
        's3_path': 'value-sets/icd_10_cm_to_ccs.csv'
    },
    {
        'name': '_value_set_icd_10_pcs_to_ccs',
        'columns': 'icd_10_pcs VARCHAR, description VARCHAR, ccs_procedure_category VARCHAR, ccs_description VARCHAR',
        'schema': 'readmissions',
        's3_path': 'value-sets/icd_10_pcs_to_ccs.csv'
    },
    {
        'name': '_value_set_potentially_planned_ccs_procedure_category',
        'columns': 'ccs_procedure_category VARCHAR, description VARCHAR',
        'schema': 'readmissions',
        's3_path': 'value-sets/potentially_planned_ccs_procedure_category.csv'
    },
    {
        'name': '_value_set_potentially_planned_icd_10_pcs',
        'columns': 'icd_10_pcs VARCHAR, description VARCHAR',
        'schema': 'readmissions',
        's3_path': 'value-sets/potentially_planned_icd_10_pcs.csv'
    },
    {
        'name': '_value_set_specialty_cohort',
        'columns': 'ccs VARCHAR, description VARCHAR, specialty_cohort VARCHAR, procedure_or_diagnosis VARCHAR',
        'schema': 'readmissions',
        's3_path': 'value-sets/potentially_planned_icd_10_pcs.csv'
    },
    {
        'name': '_value_set_surgery_gynecology_cohort',
        'columns': 'icd_10_pcs VARCHAR, description VARCHAR, ccs_code_and_description VARCHAR, specialty_cohort VARCHAR',
        'schema': 'readmissions',
        's3_path': 'value-sets/surgery_gynecology_cohort.csv'
    },
    {
        'name': '_value_set_service_categories',
        'columns': 'service_category_1 VARCHAR, service_category_2 VARCHAR',
        'schema': 'claims_preprocessing',
        's3_path': 'value-sets/service_categories.csv'
    },
]

if __name__ == "__main__":
    reference_directory = '/workspaces/synthea_dw/reference'
    directory = '/workspaces/synthea_dw/vocabulary'
    db_path = '/workspaces/synthea_dw/synthea.db'
    base_url = 'https://tuva-public-resources.s3.amazonaws.com/'
    parquet_directory = '/workspaces/synthea_dw/terminology'
    
    # Ask user for number of patients to create
    num_patients = input("Enter the number of patients to create: ")

    # Ensure input is an integer
    try:
        num_patients = int(num_patients)
        logging.info(f"Starting Synthea simulation for {num_patients} patient(s).")
        run_synthea(num_patients)
        
        logging.info("Now processing vocabulary tables.")
        process_vocabulary_tables(directory, db_path)
        
        logging.info("Now processing reference tables.")
        process_reference_tables(reference_directory, db_path)
        
        logging.info("Now processing terminology tables.")
        process_terminology_tables('terminology', terminology_tables, base_url, '/workspaces/synthea_dw/data/', db_path, parquet_directory)
        
        logging.info("Now processing value_set tables.")
        process_terminology_tables('value_sets', value_set_tables, base_url, '/workspaces/synthea_dw/data/', db_path)
    except ValueError:
        logging.error("Please enter a valid integer for the number of patients.")
        exit()