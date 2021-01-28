import requests
import json

import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

temporary = ''

class GetCovidTotals:

    def __init__(self):
        self.headers = {'x-rapidapi-key': ' API KEY HERE',
                        'x-rapidapi-host': 'covid-19-data.p.rapidapi.com'}
        self.web = 'https://covid-19-data.p.rapidapi.com/totals'

    def get_data(self):
        self.response = requests.get(self.web, headers = self.headers)
        temporary = json.loads(self.response.text)[0]
        print (temporary)

default_arguments = {
                        'owner' : 'morozovd',
                        'start_date' : days_ago(1),
                        'retries' : 1,
                        'depends_on_past' : False
                    }

dag = DAG(  dag_id = 'DAG-COVID-19', default_args = default_arguments, schedule_interval = None, catchup = False)

start = DummyOperator(task_id = 'START', dag = dag)

run_totals = PythonOperator(task_id = 'GETTING_COVID_DATA',
                            python_callable = GetCovidTotals().get_data,
                            dag = dag)

end = DummyOperator(task_id = 'END', dag = dag)

start >> run_totals >> end
