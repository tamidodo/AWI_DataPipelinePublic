"""
Last updated on Wednesday Sept 1 14:00 2021

@author: Tammy Do, Mark Ledsome

This script uses functions from the config.py file to first infer the data type
from the event payload, get the relevant configuration from the yaml file, and
then clean the uploaded file in preparation for loading into BigQuery. The
cleaned file is written to a new bucket and then the name/location of the clean
file is published as a message to a PubSub topic called QueryTriggers.

The clean function should be deployed with the trigger to run as any upload to 
the data upload bucket in Google Cloud Storage.
"""
import config
from config import gcloud_pubsub_publish

def clean_csv(event: {}, context):
    """ 
    clean_csv -- respond to Finalize/Create trigger on upload bucket 
    
    Function is triggered by a finalize/create event on a gs:storage bucket.
    The name of the file is passed as the value 
    """

    # fname - the name of the file which triggered the calling event
    fname = event['name']

    var = config.get_config(fname)
    optiom_uri1 = config.optiom_uri1(fname)
    try:
        config.df(
            var,
            config.json_schema(var['jsonfile']),
            optiom_uri1
            )

        # send notification to update the database
        topic = 'projects/awi-live/topics/update_bigquery' #from settings?
        message = fname
        gcloud_pubsub_publish(topic, message)
    except:
        config.failed_func(fname)
    return
    

def bq(event, context):
    fname = config.data_type(event)
    var = config.get_config(fname)
    try:
       config.load_bq(var, fname)
    except:
        config.failed_func(fname)
    try:
        config.update_table(var)
        config.make_view(var)
        # send notification to update the leaderboard
        topic = 'projects/awi-live/topics/update_leaderboard'
        message = 'update the leaderboard now'
        gcloud_pubsub_publish(topic, message)
    except:
        print(f"Could not complete the table merge and/or view creation for {var['name']}")
    return
