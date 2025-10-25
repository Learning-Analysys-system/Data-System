from airflow.sdk import DAG, task
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

from datetime import datetime
import sys
import os
import requests


with DAG(
    dag_id="data_pipeline",
    start_date=datetime(2025, 10, 24),
    schedule='@hourly',
    catchup=False
) as dag:

    @task
    def ingest_scorm_data():
        # Giả sử đây là hàm để lấy dữ liệu SCORM
        response = requests.get("http://host.docker.internal:5000/ingest_log_xAPI")
        print(response.json())
        if response.status_code == 200:
            return response.json()
        else:
            return []

    @task
    def loadDataToDataLake():
        # Giả sử đây là hàm để lấy dữ liệu SCORM
        response = requests.get("http://host.docker.internal:5000/load_to_datalake")
        print(response.json())
        if response.status_code == 200:
            return response.json()
        else:
            return []    

    @task
    def loadDataToDataWarehouse():
        # Giả sử đây là hàm để lấy dữ liệu SCORM
        bucket_name = 'logsystem'
        date_to_extract = datetime.now()
        clean_date = str(date_to_extract).split(' ')[0].replace('-', '/')

        response = requests.get(f"http://host.docker.internal:5000/load_to_datawarehouse?bucket_name={bucket_name}&date_to_extract={clean_date}")
        print(response.json())
        if response.status_code == 200:
            return response.json()
        else:
            return []    

    ingetsScorm = ingest_scorm_data()
    loadToDataLake = loadDataToDataLake()
    loadToDataWarehouse = loadDataToDataWarehouse()

    ingetsScorm >> loadToDataLake >> loadToDataWarehouse
