import datetime as dt
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

def print_hello():
    return 'Hello world from first Airflow DAG!'

args = {
    'owner': 'airflow', #this should be changed
    'email': ['bkaplan18@ku.edu.tr'], #this should be changed
    'email_on_failure': True,
    'email_on_retry': True,
    'depends_on_past': False,
    'start_date': dt.datetime(22,10,24),
    'max_active_runs':1,
    'email_on_success': True,
    'schedule_interval': '@daily' #scheduling the interval as daily
}


dag = DAG('hello_world', description='Hello World DAG',
          default_args = args, catchup=False)

hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)

hello_operator


#cron structure:
#Minutes [0-59]
 #  |   Hours [0-23]
  #  |   |   Days [1-31]
  #  |   |   |   Months [1-12]
   # |   |   |   |   Days of the Week [Numeric, 0-6]