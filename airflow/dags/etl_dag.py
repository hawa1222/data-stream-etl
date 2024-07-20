from airflow import DAG
from airflow.operators.python import PythonOperator
from extractors.apple_extractor import apple_extractor
from extractors.daylio_extractor import daylio_extractor
from extractors.spend_extractor import spend_extractor
from extractors.strava_extractor import strava_extractor
from extractors.youtube_extractor import youtube_extractor
from extractors.youtube_html_extractor import youtube_html_extractor
from loaders.apple_loader import apple_loader
from loaders.daylio_loader import daylio_loader
from loaders.spend_loader import spend_loader
from loaders.strava_loader import strava_loader
from loaders.youtube_loader import youtube_loader
from transformers.apple_transformer import apple_transformer
from transformers.daylio_transformer import daylio_transformer
from transformers.spend_transformer import spend_transformer
from transformers.strava_transformer import strava_transformer
from transformers.youtube_activity_transformer import youtube_activity_transformer
from transformers.youtube_html_transformer import youtube_html_transformer
from transformers.youtube_transformer import youtube_transformer
from validation.post_load_checks import post_load

default_args = {
    "owner": "hawa",
    # "start_date": datetime(2024, 7, 15, tzinfo=timezone("Europe/London")),
    # "schedule_interval": "0 0 * * 1",
    "email": ["hw97.business@proton.me"],
    "email_on_failure": True,
    "email_on_retry": False,
    # "retries": 1,
    # "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "catchup": False,
}

dag = DAG(
    dag_id="etl_dag",
    default_args=default_args,
    description="ETL DAG using Airflow",
)


apple_extract_task = PythonOperator(
    task_id="apple_extract",
    python_callable=apple_extractor,
    # execution_timeout=timedelta(minutes=10),
    dag=dag,
)

apple_transform_task = PythonOperator(
    task_id="apple_transform",
    python_callable=apple_transformer,
    # execution_timeout=timedelta(minutes=10),
    dag=dag,
)

apple_load_task = PythonOperator(
    task_id="apple_load",
    python_callable=apple_loader,
    dag=dag,
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

strava_extract_task = PythonOperator(
    task_id="strava_extract",
    python_callable=strava_extractor,
    dag=dag,
)

strava_transform_task = PythonOperator(
    task_id="strava_transform",
    python_callable=strava_transformer,
    dag=dag,
)

strava_load_task = PythonOperator(
    task_id="strava_load",
    python_callable=strava_loader,
    dag=dag,
)

youtube_html_extract_task = PythonOperator(
    task_id="youtube_html_extract",
    python_callable=youtube_html_extractor,
    dag=dag,
)

youtube_html_transformer_task = PythonOperator(
    task_id="youtube_html_transform",
    python_callable=youtube_html_transformer,
    dag=dag,
)

youtube_extract_task = PythonOperator(
    task_id="youtube_extract",
    python_callable=youtube_extractor,
    dag=dag,
)

youtube_transform_task = PythonOperator(
    task_id="youtube_transform",
    python_callable=youtube_transformer,
    dag=dag,
)

youtube_activity_transform_task = PythonOperator(
    task_id="youtube_activity_transform",
    python_callable=youtube_activity_transformer,
    dag=dag,
)

youtube_load_task = PythonOperator(
    task_id="youtube_load",
    python_callable=youtube_loader,
    dag=dag,
)

post_load_task = PythonOperator(
    task_id="post_load_checks",
    python_callable=post_load,
    dag=dag,
)


apple_extract_task >> apple_transform_task >> apple_load_task

strava_extract_task >> strava_transform_task >> strava_load_task

youtube_html_extract_task >> youtube_html_transformer_task
youtube_extract_task >> youtube_transform_task

(
    [youtube_html_transformer_task, youtube_transform_task]
    >> youtube_activity_transform_task
    >> youtube_load_task,
)


daylio_extract_task >> daylio_transform_task >> daylio_load_task

spend_extract_task >> spend_transform_task >> spend_load_task

[
    apple_load_task,
    daylio_load_task,
    spend_load_task,
    strava_load_task,
    youtube_load_task,
] >> post_load_task
