"""
### ETL DAG Tutorial Documentation
This ETL DAG is compatible with Airflow 1.10.x (specifically tested with 1.10.12) and is referenced
as part of the documentation that goes along with the Airflow Functional DAG tutorial located
[here](https://airflow.apache.org/tutorial_decorated_flows.html)
"""
# [START tutorial]
# [START import_module]
import json
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

from include import extract_json, extract_mongodb, extract_postgres


# [END import_module]

# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Mario Herrera Zamora',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
    #'retries': 0
}
# [END default_args]

# [START instantiate_dag]
with DAG(
    'etl_multi_source',
    default_args=default_args,
    description='ETL DAG tutorial',
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['ETL'],
) as dag:
    # [END instantiate_dag]
    # [START documentation]
    dag.doc_md = __doc__
    # [END documentation]


    # [START main_flow]
    start = DummyOperator(task_id="start")

    # [START section_1]
    with TaskGroup("Extract", tooltip="Data extraction") as section_1:
        task_1 = PythonOperator(task_id="MongoDB", python_callable=extract_mongodb.extact_mongodb_student)
        task_2 = PythonOperator(task_id="JSON_file", python_callable=extract_json.fetchDataToLocal)
        task_3 = PythonOperator(task_id="Postgres", python_callable=extract_postgres.extract)
        [task_1, task_2, task_3]
    # [END section_1]

    # [START section_2]
    with TaskGroup("Transform", tooltip="Data transformation process") as section_2:
        task_1 = PythonOperator(task_id="MongoDB", python_callable=extract_mongodb.sqlTransform)
        task_2 = PythonOperator(task_id="JSON_file", python_callable=extract_json.sqlTransform)
        task_3 = PythonOperator(task_id="Postgres", python_callable=extract_postgres.sqlTransform)
        [task_1, task_2, task_3]
    # [END section_2]

    # [START section_3]
    with TaskGroup("Load", tooltip="Data load") as section_3:
        task_1 = PythonOperator(task_id="MongoDB", python_callable=extract_mongodb.sqlLoad)
        task_2 = PythonOperator(task_id="JSON_file", python_callable=extract_json.sqlLoad)
        task_3 = PythonOperator(task_id="Postgres", python_callable=extract_postgres.sqlLoad)
        [task_1, task_2, task_3]
    # [END section_3]


    end = DummyOperator(task_id='end')

    start >> section_1 >> section_2 >> section_3 >> end

# [END main_flow]

# [END tutorial]