from airflow import DAG
import pendulum
from datetime import datetime,timedelta
from api.video_stats import extract_video_data, get_playlist_id, get_video_ids, save_to_json

local_tz = pendulum.timezone("Asia/Kolkata")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "kratig35@gmail.com",
    # "retries": 1,
    # "retry_delay": timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(minutes=60),
    "start_date": pendulum.datetime(2026, 8, 2, tz=local_tz),
    # "end_date": pendulum.datetime(2030, 8, 30, tz=local_tz),
}

with DAG(
    dag_id='produce_json',
    default_args=default_args,
    description='DAG to produce JSON file with raw data',
    schedule='30 21 * * *',  # Run every day at 21:30 IST (Asia/Kolkata)
    catchup=False,
) as dag: 
    # Define tasks
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extract_data= extract_video_data(video_ids)
    save_to_json_task = save_to_json(extract_data)

    # Define dependencies
    playlist_id >> video_ids >> extract_data >> save_to_json_task
    