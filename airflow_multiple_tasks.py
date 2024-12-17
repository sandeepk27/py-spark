from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Define the DAG
with DAG(
    'sample_multiple_tasks_dag',
    default_args=default_args,
    description='A sample DAG with multiple tasks',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Define Python functions for the tasks
    def task_1():
        print("Executing Task 1")

    def task_2():
        print("Executing Task 2")

    def task_3():
        print("Executing Task 3")

    # Define the tasks
    task_1 = PythonOperator(
        task_id='task_1',
        python_callable=task_1,
    )

    task_2 = PythonOperator(
        task_id='task_2',
        python_callable=task_2,
    )

    task_3 = PythonOperator(
        task_id='task_3',
        python_callable=task_3,
    )

    # Define a Bash task
    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='echo "Running Bash Task"',
    )

    # Set task dependencies
    # Task 1 and Task 2 will run in parallel
    [task_1, task_2] >> bash_task >> task_3
