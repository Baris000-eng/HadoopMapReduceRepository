import datetime as dt
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from itertools import groupby

def mapping_function(word):
    wordWithAlphabetChars = ''.join([j for j in word if j.isalpha()])
    return [wordWithAlphabetChars, 1]


def reduction_function(key, values):
    return [key, sum([  k[1] for k in values])]  # return count sum for each word


def word_count():
    with open("input.txt") as file:  # reading the txt file
        words = [my_word for line in file for my_word in line.split()]  # splitting the words

    map_res = map(mapping_function, words)  # map all words in the txt file with a count of 1.
    sor_map_res = sorted(map_res, key=lambda y: y[0])  # sort the map outcome according to words.
    result = []
    g_and_s = groupby(sor_map_res, key=lambda x: x[0])  # group the sorted outcome
    for key, value in g_and_s:
        value = list(value) #convert value var to list version of value var.
        print([key, value])
        result.append(reduction_function(key, value))  # converting grouper object to list
        # for each word, append the total word count to the result list

    for i in range(0, len(result)):
        print(str(result[i][0]) + "=>" + str(result[i][1]))  # displaying results




args = {
    'owner': 'airflow', #this should be changed
    'email': ['bkaplan18@ku.edu.tr'], #this should be changed
    'email_on_failure': True, #to make the airflow email to the specified email address when the run is a failure.
    'email_on_retry': True,#to make the airflow email to the specified email address when the run is a retry.
    'depends_on_past': False,
    'start_date': dt.datetime(22,10,24), #to set the initial date time of dag runs
    'max_active_runs':1,
    'email_on_success': True,#to make the airflow email to the specified email address when the run is a success.
    'schedule_interval': '@daily' #scheduling the interval as daily
}


dag = DAG('word_count', description='Word Count DAG',
          default_args = args, catchup=False)

word_count_operator = PythonOperator(task_id='word_count_task', python_callable=word_count, dag=dag)

word_count_operator
