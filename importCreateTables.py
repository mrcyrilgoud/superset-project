from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago
from datetime import datetime

# Define the Snowflake target tables and S3 stage
target_table_1 = "dev.raw_data.user_session_channel"
target_table_2 = "dev.raw_data.session_timestamp"
stage_location = '@dev.raw_data.blob_stage'
user_session_file = "user_session_channel.csv"
session_timestamp_file = "session_timestamp.csv"


# Task to create the Snowflake stage for the S3 bucket
@task
def create_s3_stage():
    create_stage_sql = """
        CREATE OR REPLACE STAGE dev.raw_data.blob_stage
        url = 's3://s3-geospatial/readonly/'
        file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');
    """
    SnowflakeOperator(
        task_id='create_s3_stage',
        sql=create_stage_sql,
        snowflake_conn_id='snowflake_conn'  # revert back to snowflake_conn_id
    ).execute({})


# Task to create the user_session_channel table
@task
def create_user_session_channel_table():
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS dev.raw_data.user_session_channel (
            userId INT NOT NULL,
            sessionId VARCHAR(32) PRIMARY KEY,
            channel VARCHAR(32) DEFAULT 'direct'
        );
    """
    SnowflakeOperator(
        task_id='create_user_session_channel_table',
        sql=create_table_sql,
        snowflake_conn_id='snowflake_conn'  # revert back to snowflake_conn_id
    ).execute({})


# Task to create the session_timestamp table
@task
def create_session_timestamp_table():
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS dev.raw_data.session_timestamp (
            sessionId VARCHAR(32) PRIMARY KEY,
            ts TIMESTAMP
        );
    """
    SnowflakeOperator(
        task_id='create_session_timestamp_table',
        sql=create_table_sql,
        snowflake_conn_id='snowflake_conn'  # revert back to snowflake_conn_id
    ).execute({})


# Task to load data into user_session_channel from S3
@task
def load_user_session_channel():
    load_user_session_sql = f"""
        COPY INTO {target_table_1}
        FROM {stage_location}/{user_session_file};
    """
    SnowflakeOperator(
        task_id='load_user_session_channel',
        sql=load_user_session_sql,
        snowflake_conn_id='snowflake_conn'  # revert back to snowflake_conn_id
    ).execute({})


# Task to load data into session_timestamp from S3
@task
def load_session_timestamp():
    load_session_timestamp_sql = f"""
        COPY INTO {target_table_2}
        FROM {stage_location}/{session_timestamp_file}
        FILE_FORMAT = (type = 'csv', field_optionally_enclosed_by = '"', time_format = 'AUTO');
    """
    SnowflakeOperator(
        task_id='load_session_timestamp',
        sql=load_session_timestamp_sql,
        snowflake_conn_id='snowflake_conn'  # revert back to snowflake_conn_id
    ).execute({})


# Define the DAG with a specific schedule
with DAG(
        dag_id='user_sessions_etl',
        start_date=datetime(2024, 10, 2),
        catchup=False,
        schedule_interval='30 2 * * *',  # Runs at 02:30 AM every day
        tags=['ETL'],
) as dag:
    # Task execution sequence
    create_s3_stage_task = create_s3_stage()  # Create the S3 stage first

    # Then create the tables
    create_user_session_channel_table_task = create_user_session_channel_table()
    create_session_timestamp_table_task = create_session_timestamp_table()

    # Then load data into tables
    load_user_session_channel_task = load_user_session_channel()
    load_session_timestamp_task = load_session_timestamp()

    # Set up the task dependencies:
    # First create the stage, then create tables, then load data
    create_s3_stage_task >> [create_user_session_channel_table_task, create_session_timestamp_table_task]
    create_user_session_channel_table_task >> load_user_session_channel_task
    create_session_timestamp_table_task >> load_session_timestamp_task
