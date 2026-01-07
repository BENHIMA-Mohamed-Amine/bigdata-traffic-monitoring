from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess


# Default settings for all tasks
default_args = {
    "owner": "data-engineer",
    "depends_on_past": False,  # Don't wait for previous runs
    "start_date": datetime(2026, 1, 6),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,  # Retry 3 times if task fails
    "retry_delay": timedelta(minutes=2),  # Wait 2 min between retries
}

# Define the DAG
dag = DAG(
    "traffic_data_pipeline",
    default_args=default_args,
    description="Process traffic data from HDFS to PostgreSQL",
    schedule_interval="*/3 * * * *",  # Every 10 minutes
    catchup=False,  # Don't run past missed schedules
)

# Task 1: Run Spark Processing
process_data = BashOperator(
    task_id="spark_process_traffic",
    bash_command="""
        docker exec spark-master spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark-apps/spark_processor.py
    """,
    dag=dag,
)

# Task 2: Load to PostgreSQL
load_to_db = BashOperator(
    task_id="load_to_postgres",
    bash_command="""
        docker exec spark-master spark-submit \
        --master spark://spark-master:7077 \
        --jars /opt/spark-jars/postgresql-42.6.0.jar \
        /opt/spark-apps/load_to_postgres.py
    """,
    dag=dag,
)


# Task 3: Verify data loaded (Python function)
def check_postgres_data():
    """Quick sanity check"""
    import psycopg2

    try:
        conn = psycopg2.connect(
            host="postgres",
            database="traffic_db",
            user="traffic_user",
            password="traffic_pass",
        )
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM traffic_by_zone")
        count = cursor.fetchone()[0]

        if count > 0:
            print(f"✅ Success! Found {count} zones in database")
        else:
            raise Exception("❌ No data found in traffic_by_zone!")

        conn.close()
    except Exception as e:
        print(f"❌ Verification failed: {e}")
        raise


verify_data = PythonOperator(
    task_id="verify_data_loaded",
    python_callable=check_postgres_data,
    dag=dag,
)

# Define task dependencies (execution order)
process_data >> load_to_db >> verify_data
