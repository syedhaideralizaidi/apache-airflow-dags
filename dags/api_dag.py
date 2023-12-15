import json
from datetime import datetime
from airflow.models import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator

with DAG(
    dag_id='api_dag',
    start_date=datetime(2023,12,15),
    schedule_interval='*/3 * * * *',
    catchup=False
) as dag:
    task_api_is_active = HttpSensor(
        task_id='is_api_active',
        http_conn_id='api_posts',
        endpoint='posts/'
    )
    token="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwiZXhwIjoxNzAyNjQ3NTE3LCJpYXQiOjE3MDI2MzU1MTcsImp0aSI6ImFmZjUyNjdkMjJmZDQyOGQ4NjUyNDk4ODg1ZGIzN2NiIiwidXNlcl9pZCI6Mn0.YGm5L4TaXIerd8hDCQsZC-j2CRD_CzKG8luEIwfKNHM"
    task_get_posts = SimpleHttpOperator(
        task_id='get_posts',
        http_conn_id='api_posts',
        endpoint='/auth/login/',
        method='POST',
        data={"email":"haider.zaidiy+1@gmail.com", "password":"Conovo@123"},
        #headers={"Authorization": f"Bearer {token}"},
        #method='GET',
        response_filter = lambda r: json.loads(r.text),
        log_response=True
    )

