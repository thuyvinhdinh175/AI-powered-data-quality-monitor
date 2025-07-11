"""
Airflow DAG for data ingestion and quality checks

This DAG:
1. Ingests data from various sources
2. Validates data quality using Great Expectations
3. Generates LLM insights on validation results
4. Sends alerts for failed validations
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

import sys
import os

# Add the project directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Import custom modules
from llm_agent.insight_generator import generate_llm_insights
from llm_agent.fix_suggestor import suggest_fixes
from app.alert_manager import send_alerts


default_args = {
    'owner': 'data_quality',
    'depends_on_past': False,
    'email': ['your_email@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_quality_pipeline',
    default_args=default_args,
    description='Data ingestion and quality check pipeline',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=['data-quality'],
)

# Task: Ingest data
ingest_data = BashOperator(
    task_id='ingest_data',
    bash_command='python -m app.data_ingestion.ingest --source=api --target=data/raw/{{ ds }}/',
    dag=dag,
)

# Task: Run Great Expectations validation
run_validation = BashOperator(
    task_id='run_validation',
    bash_command='python -m app.validator.run_checks --dataset=data/raw/{{ ds }}/ --suite=daily',
    dag=dag,
)

# Task: Generate LLM insights based on validation results
def _generate_insights(**kwargs):
    validation_results_path = f"data/validation_results/{{ ds }}/results.json"
    return generate_llm_insights(validation_results_path)

generate_insights = PythonOperator(
    task_id='generate_insights',
    python_callable=_generate_insights,
    dag=dag,
)

# Task: Suggest fixes for failed validations
def _suggest_fixes(**kwargs):
    validation_results_path = f"data/validation_results/{{ ds }}/results.json"
    return suggest_fixes(validation_results_path)

suggest_dq_fixes = PythonOperator(
    task_id='suggest_dq_fixes',
    python_callable=_suggest_fixes,
    dag=dag,
)

# Task: Send alerts for failed validations
def _send_alerts(**kwargs):
    validation_results_path = f"data/validation_results/{{ ds }}/results.json"
    insights_path = f"data/insights/{{ ds }}/insights.json"
    fixes_path = f"data/fixes/{{ ds }}/fixes.json"
    return send_alerts(validation_results_path, insights_path, fixes_path)

send_dq_alerts = PythonOperator(
    task_id='send_dq_alerts',
    python_callable=_send_alerts,
    dag=dag,
)

# Define task dependencies
ingest_data >> run_validation >> generate_insights >> suggest_dq_fixes >> send_dq_alerts
