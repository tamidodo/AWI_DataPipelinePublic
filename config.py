"""
Last updated on Monday July 7 14:00 2021

@author: Tammy Do, Mark Ledsome

This file contains general functions for cleaning data and schema creation, as
well as a message publishing function and a failsafe function. Due to the
similarities between data types, we have chosen to write the functions such 
that they are suitable for all data types and take arguments which provide 
type-specific modifications where necessary.
"""

import json, yaml, base64
import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.cloud import storage
from google.cloud import pubsub
import logging

from google.cloud import logging as cloudlogging

# Setting up the cloud logger to have more control over logging
log_client = cloudlogging.Client()
log_handler = log_client.get_default_handler()
cloud_logger = logging.getLogger("cloudLogger")
cloud_logger.setLevel(logging.INFO)
cloud_logger.addHandler(log_handler)


def strip_excel(s):
    """Strips Excel artifacts from data set"""
    return s.strip('"=')


def optiom_uri1(data_type: str) -> str:
    """Generates uri for optiom files"""
    if "ProductionRpt" in data_type:
        return(f'gs://upload_updates/{data_type}')
    else:
        return('No')


def get_config(data_type: str) -> []:
    """Returns a list of dictionaries containing config settings"""
    stream = open('settings.yaml', 'r')
    dictionary = yaml.load_all(stream, Loader=yaml.FullLoader)
    if "ProductionRpt" in data_type:
        data_type = "ProductionRpt.xlsx"
    for doc in dictionary:
        if doc['name'] == data_type:
            return doc


def data_type(event: {}) -> str:
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    return pubsub_message


def json_schema(fname: str) -> str:
    """Returns schema from the gs schema file."""
    with open(fname, 'r') as f:
        return json.load(f)


def col_names(json_schema: str) -> []:
    """Return column names from schema"""
    return [col.get("name") for col in json_schema]


def bq_types(json_schema: str) -> []:
    """Return bigquery column types from schema"""
    return [col.get("type") for col in json_schema]


def bq_schema(col_names: [], bq_types: []) -> []:
    """Return the bigquery schema for the table update"""
    a = ['bigquery.SchemaField("'+x+'", "'+y+'")' for (x,y) in zip(col_names, bq_types)]
    return [eval(z) for z in a]


def type_defs(json_schema: str) -> {}:
    """Return dict containing df column:type pairs mapped from gs_schema"""
    gs_to_pd = {
        'STRING': 'str',
        'NUMERIC': 'float64',
        'FLOAT': 'float64',
        'INTEGER': 'float64',
        'BOOLEAN': 'str'
    }
    a = {col.get("name"): gs_to_pd.get(col.get("type")) for col in json_schema}
    return {k:v for (k,v) in a.items() if v != None}


def dates(json_schema: str) -> []:
    """Find columns that have 'date type' to be parsed when reading csv"""
    return [col.get("name") for col in json_schema if col.get("type") == 'DATE']


def convert(cf_dict: {}) -> {}:
    """Takes converter functions dictionary and evaluates the string value as a function name"""
    return {k:eval(v) for (k,v) in cf_dict.items()}
        

def df(var: {}, json_schema: str, optiom_uri1: str):
    """Reads in CSV to a pd dataframe (df) and cleans it before writing df to a new CSV"""
    cloud_logger.info(f"Running clean_csv for {var['name']}")
    if "ProductionRpt" in optiom_uri1:
        prime = pd.read_excel(
            optiom_uri1,
            sheet_name='Prime Production Report',
            engine='openpyxl'
        )
        plus = pd.read_excel(
            optiom_uri1,
            sheet_name='Plus Production Report',
            engine='openpyxl'
        )
        prime = prime.assign(SOURCE = 'prime')
        plus = plus.assign(SOURCE = 'plus')
        prime.insert(17, 'DEALER', np.nan)
        prime.insert(18, 'DEALER CONTACT', np.nan)
        prime.insert(33, 'AMOUNT COLLECTED BY DEALER',np.nan)
        prime.insert(38, 'REFERRAL FEE NET', np.nan)
        prime.insert(39, 'REFERRAL FEE GST', np.nan)
        prime.insert(40, 'REFERRAL FEE TOTAL', np.nan)
        prime.insert(41, 'PAYABLE BY DEALER TO SELLER', np.nan)
        df = pd.concat([prime,plus])
    
    else:
        df = pd.read_csv(
            var['uri1'],
            converters=convert(var['convert_funcs']),
            parse_dates=dates(json_schema),
            usecols=col_names(json_schema),
            low_memory=False
            )
        df = df.dropna(subset=[var['date_col']])
        df["PolicyEffectiveDate"] = pd.to_datetime(
            df["PolicyEffectiveDate"],
            errors='coerce'
            )
        df = df.replace('', np.NaN)
        df = df.astype(type_defs(json_schema))
    
    df.to_csv(var['uri2'], index=False)
    cloud_logger.info(f"{var['name']} cleaned successfully")
    return


def load_bq(var: [], fname: str):
    """Loads data from CSV into a Bigquery table then deletes raw file"""
    cloud_logger.info(f"Loading data for {var['name']} into BQ update table")
    client = bigquery.Client()
    strg_clnt = storage.Client()
    src_bkt = strg_clnt.get_bucket('upload_updates')
    
    cols = col_names(json_schema(var['jsonfile']))
    schema1 = bq_schema(cols, bq_types(json_schema(var['jsonfile'])))

    load_job = client.load_table_from_uri(
            var['uri2'],
            var['table_new'],
            job_config=bigquery.LoadJobConfig(
                    schema=schema1,
                    skip_leading_rows=1,
                    source_format=bigquery.SourceFormat.CSV,
                    allow_quoted_newlines=True,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
                    )
        )
    load_job.result()  # Waits for the job to complete.
    destination_tbl = client.get_table(var['table_new'])
    cloud_logger.info(f"Loaded {destination_tbl.num_rows} rows into {var['table_new']}.")
    # delete in old destination
    src_bkt.blob(fname).delete()
    return


def grab_date(new_table: str, date_col: str):
    client = bigquery.Client()
    date_query = f'SELECT MIN ({date_col}) AS mindate FROM {new_table}'
    res = client.query(date_query)
    res.result()
    for row in res:
        start_date = row.mindate
    return(start_date)


def update_query(new_table: str, old_table: str, date_col: str, start_date: str) -> str:
    """Creates the query for merging the updated and base tables"""
    return(
        f"CREATE OR REPLACE TABLE {old_table} AS (\n\
        SELECT * FROM {old_table} where {date_col} < '{start_date}' \n\
        UNION ALL\n\
        SELECT * FROM {new_table}\n\
        ORDER BY {date_col} ASC\n\
        )"
    )

def update_table(var: []):
    """Merges the base and current tables in Bigquery"""
    new_table = var['table_new']
    old_table = var['table_old']
    date_col = var['date_col']
    start_date = grab_date(new_table, date_col)
    client = bigquery.Client()
    union_query = update_query(new_table, old_table, date_col, start_date)
    query_job = client.query(union_query)
    query_job.result()
    cloud_logger.info(f"{old_table} merged with {new_table} with cutoff date {start_date}")
    return


def make_view(var: []):
    """Creates GDS view from base table in Bigquery"""
    client = bigquery.Client()
    query_job = client.query(var['view_query'])
    query_job.result()
    cloud_logger.info(f"GDS view for {var['name']} updated")
    return

def failed_func(fname):
    """Moves raw CSV into error bucket if main functions fail"""
    strg_clnt = storage.Client()
    src_bkt = strg_clnt.get_bucket('upload_updates')
    src_bkt.copy_blob(
        src_bkt.blob(fname),
        strg_clnt.get_bucket('awi_error'),
        fname
    )
    src_bkt.blob(fname).delete()
    cloud_logger.warning('function failed')
    return


# note - I'm making some changes here for the production 
def gcloud_pubsub_publish(topic: str, message: str):
    """Publish a message to the specified topic"""
    print(f"Sending message: {message} to topic: {topic}")

    publisher = pubsub.PublisherClient()
    future = publisher.publish(topic, message.encode("utf-8"))
    future.result()
    return

