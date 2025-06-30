from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from scan_dag.tasks.scan_with_noseyparker import scan_repo_with_noseyparker
from scan_dag.tasks.process_data import process_data
from scan_dag.tasks.analyze_results_with_llm import analyze_results_with_llm
from scan_dag.tasks.send_results_to_pg import send_results_to_pg


default_args = {
    'owner': 'security-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': lambda context: print(f"Task failed: {context.get('task_instance').task_id}"),
}

def process_repo(**context):
    params = context['params']
    return params.get('repo_url')


with DAG(
    'scan_dag',
    default_args=default_args,
    schedule_interval='@hourly',
    start_date=datetime(2025, 1, 1),
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
    base_report_path = Variable.get("SECRET_SCAN_REPORT_PATH", '/opt/airflow/results/scan')

    repo_url = PythonOperator(
        task_id='process_repo',
        python_callable=process_repo,
        provide_context=True,
    )

    scan_task = scan_repo_with_noseyparker(f"{repo_url.output}.git", f"{base_report_path}.json")

    process_data_task = process_data(f"{base_report_path}.json")

    analyze_llm_task = analyze_results_with_llm(process_data_task)
    send_results_task = send_results_to_pg(f"{base_report_path}.json")

    repo_url >> scan_task >> process_data_task >> analyze_llm_task >> send_results_task
