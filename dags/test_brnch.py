from airflow.decorators import dag, task
from airflow.operators.python import BranchPythonOperator
from datetime import datetime



@dag(
    start_date=datetime(2025,1,1),
    schedule_interval=None,
    catchup=False,
)
def branching_example():

    @task.branch(task_id='task')
    def first_task():
        t = 5
        if t > 6:
            return 'task1'
        else:
            return 'task2'
    
    @task(task_id = 'task1')
    def task1():
        return "task1"
    
    @task(task_id = 'task2')
    def task2():
        return "task2"

    @task(trigger_rule = 'none_failed_or_skipped')
    def task3(val):
        print(f"Hello from {val}")

    val = first_task() >> [task1(), task2()]
    val >> task3(val[0] if val[0] else val[1])
    
 


dag = branching_example()