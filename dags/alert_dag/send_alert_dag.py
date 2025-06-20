from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
from alert_dag.tasks.send_alert import send_data
from alert_dag.tasks.get_data import get_data
from alert_dag.tasks.transform_data import transform_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False
}

# Use ExternalTaskSensor to listen to the Parent_dag and cook_dinner task
# when cook_dinner is finished, Child_dag will be triggered
wait_for_scan = ExternalTaskSensor(
    task_id='send_alert',
    external_dag_id='scan_dag',
    external_task_id='send_results_to_pg'
)

with DAG(
    'k8s_secret_scan',
    default_args=default_args,
    schedule_interval='@hourly',
    max_active_runs=1,
    catchup=False,
    tags=['security', 'scanning'],
    doc_md="""### Secret Scanning DAG\n
    This DAG performs automated secret scanning on repositories using:
    1. NoseyParker for initial scanning
    2. Data processing
    3. LLM analysis for false positive reduction
    4. PostgreSQL storage of results
    """
) as dag:
    data = get_data()
    transform_data = transform_data(data)
    send_data_task = send_data(transform_data)
    wait_for_scan >> get_data() >> transform_data() >> send_data_task