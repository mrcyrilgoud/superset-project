from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import logging


def return_snowflake_conn():
    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    # Get connection and cursor
    conn = hook.get_conn()
    return conn.cursor()


@task
def run_ctas(table, select_sql, primary_key=None):
    logging.info(f"Creating table: {table}")
    logging.info(f"Executing SQL: {select_sql}")

    cur = return_snowflake_conn()

    try:
        # Begin transaction
        cur.execute("BEGIN;")
        # Create or replace the table
        sql = f"CREATE OR REPLACE TABLE {table} AS {select_sql}"
        logging.info(f"SQL to execute: {sql}")
        cur.execute(sql)

        # Check primary key uniqueness if a primary key is provided
        if primary_key:
            sql = f"SELECT {primary_key}, COUNT(1) AS cnt FROM {table} GROUP BY 1 HAVING COUNT(1) > 1"
            logging.info(f"Checking uniqueness with SQL: {sql}")
            cur.execute(sql)
            result = cur.fetchone()

            if result:
                logging.error(f"Primary key uniqueness failed for {primary_key}. Duplicate found: {result}")
                raise Exception(f"Primary key uniqueness failed: {result}")

        # Commit the transaction if no errors
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        logging.error(f"Failed to create table {table}. Error: {e}. ROLLBACK completed!")
        raise


# DAG definition
with DAG(
        dag_id='BuildELT_JoinedTables',
        start_date=datetime(2024, 10, 2),
        catchup=False,
        tags=['ELT'],
        schedule_interval='45 2 * * *'
) as dag:
    # Table and SQL for creating session_summary
    table = "dev.analytics.session_summary"
    select_sql = """
    SELECT u.*, s.ts
    FROM dev.raw_data.user_session_channel u
    JOIN dev.raw_data.session_timestamp s
    ON u.sessionId = s.sessionId
    """

    # Task to run the CTAS operation
    create_session_summary = run_ctas(table, select_sql, primary_key='sessionId')