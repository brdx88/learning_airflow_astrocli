from airflow.sdk import dag, task
from pendulum import datetime               # For Airflow DAGs, you should use pendulum.datetime as it avoids common timezone issues and is the recommended approach in Airflow documentation. `datetime.datetime` is less robust its timezone support.

# 1) Defining the DAG
@dag(
    schedule = "@daily",
    start_date = datetime(2025, 1, 1),
    description = "DAG to check data",
    tags = ["learning", "testing", "check", "data"],
    max_consecutive_failed_dag_runs = 3,
)

def check_dag():
    
    # 2) Creating the Tasks
    @task.bash
    def create_file():
        return "echo 'Hi there!' > /tmp/dummy"          # to create a file dummy in the `tmp` directory with "Hi there!".
    
    @task.bash
    def check_file_exists():
        return "test -f /tmp/dummy"                     # to verify that the file dummy exists in the `tmp` directory.

    @task
    def read_file():
        print(open('/tmp/dummy','rb').read())           # to read and print on the standard output the content of the dummy file.

    create_file() >> check_file_exists() >> read_file()

# 3) Instantiating the DAG
check_dag()