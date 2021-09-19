## allwest.ai - Allwest Intelligence
AWI is a 'lightweight' business intelligence system for Allwest Insurance.  

We define a pipeline whereby various data sources are cleaned and processed for analysis and visualization using Google Data Studio and Jupyter Notebooks.

### Table of contents
* [General info](#general-info)
* [Technologies](#technologies)
* [Specifics](#specifics)
* [Status](#status)
* [Collaborators](#collaborators)

## General info
Typically the data sources begin with some sort of relational system belonging to an insurance partner, and end up as non-normalized data tables stored in the data warehouse.  This system ultimately generates table and views in Google Big Query, which is Google's cloud-based data warehouse product. 

A typical pipeline would progress as follows: 

1. User uploads raw .CSV update to an 'upload_data_here' bucket in Google Storage.
2. The upload triggers a Google Cloud Function (clean) that processes the data, writes the "cleaned" version to a different bucket and publishes a message to a PubSub topic indicating what data type was cleaned. 
3. The PubSub message triggers another Google Cloud Function (bq) that loads the cleaned data to a "current" BigQuery table, executes an "update table" query to merge the base and current tables, then executes a "make view" query to further process the data and generate one or more "Views" to be used in Google Data Studios. Because the queries and analysis are individual to each data type, the SQL statements are defined in the settings.yaml file.

The pipeline is similar for each of the data sources so we have designed a system that generalizes the data update process.  Configuration parameters for the cleaning function are defined in settings.yaml, configuration parameters for the bq function are defined in settings.yaml and data schemas are defined in a .json file, one for each data type.  

The advantage of this approach is that it keeps the code base as small and "DRY" as possible, and configuration is the same for all the data sources.  We thus minimize the amount of ongoing support and testing that is required if new data sources are introduced. 

## Technologies
* Google Cloud - (Storage, Cloud Functions, Biq Query, Pub/Sub)
* Google Data Studio
* Python
* Jupyter
* Pandas
* SQL

## Configuration
The file settings.yaml contains configuration information for the system.  There is a yaml document for each data type of data that can be processed, and the file config.py contains the code that processes the configuration information. For any Google Cloud Function, two arguments for the main function are provided by Google called "event" and "context" which are essentially a payload for metadata about the triggering event. In the main.py script, data type is inferred from the name of the file uploaded (which is pulled from the event payload).

## Specfics
Current data types used are Transactions and Retention. CSV file names are based on the automatic file names used when downloading CSVs from "autolink" (eg. ProductivityReport.CSV for transactions or RenewalList.CSV for retention) and the upload bucket is the same for all data types. If the clean is successful, a new CSV is generated with the same name but in the bucket called awi-data. If the clean is unsuccessful, the original CSV is preserved and archived in the "error_upload" bucket.

In Bigquery, there is one dataset called "Database". Each data type has two data tables (base and current) stored in this dataset, along with any Google Data Studio views generated from the tables. The base tables contain all historical data and are used to generate the GDS views. The current tables are the ones that have fresh data pushed into them. There are SQL statements in the queries.yaml file that are run by the bq function to append the current table to the base one when there is fresh data. The bq function waits for the first merging query to finish then runs the next query to generate specific views of the base table that only look at a subset of columns and do some analysis between columns to generate calculated fields which are used in Google Data Studio to visualize the data.

### Command Line Access

Google provides various command line tools that can be run from a shell to access and manipulate resources in Google cloud.

We can use the command line tool "bq" to examine AWI's datasets and tables in GBQ, and to perform operations such as executing queries.    The following brief 'cheat sheet' contains a few useful commands.  More detailed documentation for the bq utility is available online, and via:

```bq --help```

| Command | Description |
|:--|:--|
| ```bq shell```  | Enter the shell |
| ```ls``` | List datasets |
| ```show GDS``` | Show metadata about the dataset |
| ```show GDS.TRANSACTIONS``` | Show metadata about the table|
| ```query "select * from GDS.TRANSACTIONS order by EntryDateTime desc limit 10"``` | Show the most recent 10 transactions |
| ```bq query --use_legacy_sql=false < testq.sql``` | Execute query contained in sql file |


## Status
Project is: _completed_ pending further possible additional features

## Collaborators
Created by Tammy Do and Mark Ledsome
