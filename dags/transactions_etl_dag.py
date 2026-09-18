from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="card_transaction_etl",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "etl", "card-transactions"],
) as dag:

    run_spark_etl = BashOperator(
        task_id="run_spark_etl",
        bash_command=(
            "cd /opt/airflow && "
            "python /opt/airflow/spark_jobs/transactions_etl.py"
        ),
    )

    validate_outputs = BashOperator(
        task_id="validate_outputs",
        bash_command=(
            "test -d /opt/airflow/data/curated/clean_transactions && "
            "test -d /opt/airflow/data/curated/customer_monthly_spend && "
            "test -d /opt/airflow/data/curated/fraud_by_city"
        ),
    )

    run_spark_etl >> validate_outputs