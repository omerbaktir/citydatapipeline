from airflow.decorators import task
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

# Default arguments for the Snowflake task
default_args = {
    'snowflake_conn_id': 'snowflake_default',  # Using the Snowflake connection ID
}

@task
def run_requests_copy():
    today = datetime.now().strftime('%m%d%Y')
    file_path = f'path/nyc_311_requests_{today}.csv'

    # Construct the COPY INTO command
    copy_command = f"""
    COPY INTO DB.SCHEMA.service_requests
    FROM (
      SELECT 
    $1 as unique_key,
    $2 as created_date,        
    $3 as closed_date,       
    $4 as agency,
    $6 as complaint_type,
    $7 as descriptor,
    $8 as location_type,
    $9 as incident_zip,
    $17 as city,
    $20 as status,
    $21 as due_date,
    $26 as borough,
    $29 as data_channel_type,
    $39 as latitude,
    $40 as longitude 
    FROM @aws_stage
    )
    PATTERN = '{file_path}'
    ON_ERROR = 'CONTINUE';
    """
    
    # Execute the Snowflake command
    copy_task = SnowflakeOperator(
        task_id='run_requests_copy',
        sql=copy_command,
        snowflake_conn_id='snowflake_default',
    )
    
    return copy_task.execute(context={})  # Call execute method to run task

# Now this function can be added as part of a DAG group or called from another DAG
