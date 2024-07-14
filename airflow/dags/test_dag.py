from airflow import DAG
from airflow.operators.python import PythonOperator
from extractors.daylio_extractor import daylio_extractor
from extractors.spend_extractor import spend_extractor
from loaders.daylio_loader import daylio_loader
from loaders.spend_loader import spend_loader
from transformers.daylio_transformer import daylio_transformer
from transformers.spend_transformer import spend_transformer

default_args = {
    "owner": "hawa",
    "depends_on_past": False,
    "catchup": False,
}

dag = DAG(
    dag_id="test_dag",
    default_args=default_args,
    description="Testing DAG using Airflow",
)


daylio_extract_task = PythonOperator(
    task_id="daylio_extract",
    python_callable=daylio_extractor,
    dag=dag,
)

daylio_transform_task = PythonOperator(
    task_id="daylio_transform",
    python_callable=daylio_transformer,
    dag=dag,
)

daylio_load_task = PythonOperator(
    task_id="daylio_load",
    python_callable=daylio_loader,
    dag=dag,
)

spend_extract_task = PythonOperator(
    task_id="spend_extract",
    python_callable=spend_extractor,
    dag=dag,
)

spend_transform_task = PythonOperator(
    task_id="spend_transform",
    python_callable=spend_transformer,
    dag=dag,
)

spend_load_task = PythonOperator(
    task_id="spend_load",
    python_callable=spend_loader,
    dag=dag,
)


daylio_extract_task >> daylio_transform_task >> daylio_load_task
spend_extract_task >> spend_transform_task >> spend_load_task
