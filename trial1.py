# Airflow Schema Workflow Goal

# Phase 1
# - fetch the API from rapidapi
# - transform it into csv file
# - save data to csv file
# - upload csv into the GCP Cloud


# Step 1: Library

from airflow import DAG
import request
import json
import pandas as pd

from datetime import date, datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Get URL (How to see the type and value of the data?)
url = "https://hotels4.p.rapidapi.com/locations/v2/search"

querystring = {"query":"new york","locale":"en_US","currency":"USD"}

headers = {
    'x-rapidapi-host': "hotels4.p.rapidapi.com",
    'x-rapidapi-key': "SIGN-UP-FOR-KEY"
    }

response = requests.request("GET", url, headers=headers, params=querystring)

# Step 2 : Args (dictionary)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 8, 13),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

# Step 3 : DAG Objects

dag = DAG(
    "ingest",
    default_args=default_args, 
    schedule_interval=timedelta(1)
    )


# Step 4 : Tasks (mostly still copy pasting but still don't know how to adjust it)

# importing to json
def import_data():
    url = config.response
    data_req = requests.get(url)
    data_json = data_req.json()
    return data_json

# copy paste from the example but still don't know how?
def transform_data(data_json):
    dataframe = pd.DataFrame(data_json['data'])
    df = dataframe.rename(columns = rename_header)
    df['timestamp'] = df['date_time'].copy()
    df = df[reposition_header]
    # convert timezone(UTC) to local time(Jakarta)
    df.date_time = pd.to_datetime(df.date_time, unit="ms")
    df.date_time = df.date_time.dt.tz_localize('UTC').dt.tz_convert('Asia/Jakarta').apply(lambda d: d.replace(tzinfo=None))
    df['weekday'] = df['date_time'].dt.day_name()
    df['timestamp'] = df['timestamp']/1000
    df['timestamp'] = df['timestamp'].astype(int)
    df = df.fillna(0)
    df['traffic_index_weekago'] = df['traffic_index_weekago'].astype(int)
    return df

# same, still copy pasting, but basically it should transform the transformed data from json to csv?
def save_new_data_to_csv(data_to_append, fetch_date):
    filename = get_file_path(fetch_date)
    if not data_to_append.empty:
        data_to_append.to_csv(filename, encoding='utf-8', index=False)

# Step 5 : Tasks Dependencies

import_data >> transform_data >> save_new_data_to_csv
