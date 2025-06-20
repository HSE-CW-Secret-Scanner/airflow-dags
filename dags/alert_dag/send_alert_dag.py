from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor

from alert_dag.tasks.get_data import get_data
from alert_dag.tasks.send_data import send_data

default_args = {
    'owner': 'security-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'alert_dag',
    default_args=default_args,
    schedule_interval='@hourly',
    max_active_runs=1,
    catchup=False,
    tags=['security', 'scanning'],
    doc_md="""DAG for security alerts triggered after successful scans"""
) as dag:
    # Wait for scan_dag to complete successfully
    wait_for_scan = ExternalTaskSensor(
        task_id='wait_for_scan_completion',
        external_dag_id='scan_dag',
        external_task_id=None,  # Wait for entire DAG, not specific task
        allowed_states=['success'],
        execution_delta=timedelta(minutes=1),  # Allow short delay
        mode='reschedule',
        timeout=3600,  # 1 hour timeout
        poke_interval=300,  # Check every 5 minutes
    )

    get_data_task = get_data()
    send_data_task = send_data(get_data_task)

    wait_for_scan >> get_data_task >> send_data_task
