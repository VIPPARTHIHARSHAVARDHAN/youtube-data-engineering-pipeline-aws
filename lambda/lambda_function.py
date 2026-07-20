import awswrangler as wr
import pandas as pd
import urllib.parse
import os

print("Imports successful")

os_input_s3_cleansed_layer = os.environ['s3_cleansed_layer']
os_input_glue_catalog_db_name = os.environ['glue_catalog_db_name']
os_input_glue_catalog_table_name = os.environ['glue_catalog_table_name']
os_input_write_data_operation = os.environ['write_data_operation']

print("Environment variables loaded")

def lambda_handler(event, context):

    print("Lambda started")

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(
        event['Records'][0]['s3']['object']['key'],
        encoding='utf-8'
    )

    print("Bucket:", bucket)
    print("Key:", key)

    print("Reading JSON...")
    df_raw = wr.s3.read_json(f"s3://{bucket}/{key}")
    print("JSON Read Successfully")

    print("Normalizing JSON...")
    df_step_1 = pd.json_normalize(df_raw['items'])
    print("Normalization Complete")

    print("Writing Parquet...")
    wr_response = wr.s3.to_parquet(
        df=df_step_1,
        path=os_input_s3_cleansed_layer,
        dataset=True,
        database=os_input_glue_catalog_db_name,
        table=os_input_glue_catalog_table_name,
        mode=os_input_write_data_operation
    )

    print("Parquet Written")

    return wr_response
