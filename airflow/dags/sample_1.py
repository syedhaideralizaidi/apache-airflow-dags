from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import subprocess

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define a function to execute Scrapy spider
def run_scrapy_spider():
    # Command to run the Scrapy spider
    command = f"cd /home/ahsan/airflow/dags/scraper/tutorials && scrapy crawl MercadoagrarioSpider"
    
    # Execute the Scrapy spider using subprocess
    subprocess.run(command, shell=True)

# Define the Airflow DAG
dag = DAG(
    'scrapy_airflow_integration',
    default_args=default_args,
    description='Run Scrapy spider with Airflow',
    schedule_interval=timedelta(days=1),  # Set your desired schedule
)

# Define the task that runs the Scrapy spider using PythonOperator
run_scrapy_task = PythonOperator(
    task_id='run_scrapy_task',
    python_callable=run_scrapy_spider,
    dag=dag,
)

# Define the task dependencies
run_scrapy_task
