from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
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

with DAG(
    'secret_scan',
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
    repo_url = Variable.get("SECRET_SCAN_REPO_URL")
    base_report_path = Variable.get("SECRET_SCAN_REPORT_PATH")

    scan_task = scan_repo_with_noseyparker(repo_url, f"{base_report_path}.json")

    process_data_task = process_data(f"{base_report_path}.json")

    analyze_llm_task = analyze_results_with_llm(process_data_task)
    send_results_task = send_results_to_pg(f"{base_report_path}.json")

    scan_task >> process_data_task >> analyze_llm_task >> send_results_task
