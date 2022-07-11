from airflow import DAG
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator


from datetime import datetime, timedelta

default_args = {
    'owner' : 'AlonsoMD',
    'start_date': datetime(2022, 7, 10),
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'dataflow_default_options': {
        'project' : 'protean-tome-355920',
        'region' : 'us-east1',
        'runner': 'DataflowRunner'
    }
}


with DAG(
    'loanApplications_DAG',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False) as dag:
    
    process = DataFlowPythonOperator(
        task_id="cleanloandata",
        py_file='gs://us-east1-composer-md-f5ea2d67-bucket/process_data.py',
        options={'input': 'gs://md_pe_datapipeline/data_in/loanApplications.csv'}
    )
    
    