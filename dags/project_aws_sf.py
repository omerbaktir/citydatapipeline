from airflow import DAG
from airflow.utils.task_group import TaskGroup
from group.project_aws_sf.api_call_violations import api_call_violations
from group.project_aws_sf.api_call_311 import api_call_311
from group.project_aws_sf.snowflake_violations import run_parking_violations_copy
from group.project_aws_sf.snowflake_requests import run_requests_copy
from airflow.decorators import task
from datetime import datetime

# Define the main DAG
with DAG(dag_id='main_dag_aws_sf',
         start_date=datetime(2023, 9, 16),
         schedule_interval='@daily',
         catchup=False) as dag:

    # Define a start task if needed
    @task
    def start_task():
        print("Starting the DAG")

    # Define an end task if needed
    @task
    def end_task():
        print("Ending the DAG")

    # Create a task group for the tasks
    with TaskGroup(group_id='api_tasks_group') as api_tasks_group:
        task_1 = api_call_violations()  # Task 1 from the group/tasks/task_1.py
        task_2 = api_call_311()  # Task 2 from the group/tasks/task_2.py

        # Set dependencies within the group
        task_1 >> task_2
    
    with TaskGroup(group_id='snowflake_tasks_group') as snowflake_tests_group:
        task_1 = run_parking_violations_copy()
        task_2 = run_requests_copy()
        
        task_1 >> task_2

    # Define dependencies for the entire DAG
    start_task() >> api_tasks_group >> snowflake_tests_group >> end_task()
