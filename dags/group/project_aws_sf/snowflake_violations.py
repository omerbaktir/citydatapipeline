from airflow.decorators import task
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

# Default arguments for the Snowflake task
default_args = {
    'snowflake_conn_id': 'snowflake_default',  # Using the Snowflake connection ID
}

@task
def run_parking_violations_copy():
    today = datetime.now().strftime('%m%d%Y')
    file_path = f'path/nyc_parking_violations_{today}.csv'

    # Construct the COPY INTO command
    copy_command = f"""
    COPY INTO DB.Schema.parking_violations
    FROM (
      SELECT 
        $1 AS plate, 
        $2 AS state_code,
        $3 AS licence_type,
        TO_DATE($5, 'MM/DD/YYYY') AS issue_date,
        TRY_TO_TIME (trim($6), 'HH12:MI AM') AS violation_time,
        $7 AS violation_type,
        $9 AS fine_amount,
        $13 AS payment_amount,
        $14 AS amount_due,
        $15 AS precint,
        $16 AS county,
        $17 as issuing_agency
      FROM @AWS_stage
    )
    PATTERN = '{file_path}'
    ON_ERROR = 'CONTINUE';
    """
    
    # Execute the Snowflake command
    copy_task = SnowflakeOperator(
        task_id='run_copy_into_parking_violations',
        sql=copy_command,
        snowflake_conn_id='snowflake_default',
    )
    
    return copy_task.execute(context={})  # Call execute method to run task

# Now this function can be added as part of a DAG group or called from another DAG
