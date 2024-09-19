from airflow.decorators import task
import requests
import csv
from datetime import datetime, timedelta
from airflow.hooks.S3_hook import S3Hook
from io import StringIO

@task
def api_call_violations():
    # Define the API endpoint
    url = 'https://data.cityofnewyork.us/resource/nc67-uf89.json'
    
    def format_violation_time(time_str):
        try:
            # Ensure the input is at least 5 characters and ends with 'A' or 'P'
            if len(time_str) < 5 or time_str[-1] not in ['A', 'P']:
                raise ValueError("Invalid format")

            # Add a space before the AM/PM part
            formatted_time = time_str[:-1] + " " + ("AM" if time_str[-1] == 'A' else "PM")
            return formatted_time
        except Exception:
            # Return the original string if there is an error
            return time_str    

    # Get today's date and the date 10 days ago
    today = datetime.now()
    ten_days_ago = today - timedelta(days=10)

    # Format today's date for the filename: MMDDYYYY
    today_str_for_filename = today.strftime('%m%d%Y')

    # Format 10 days ago as MM/DD/YYYY for the API query
    ten_days_ago_str = ten_days_ago.strftime('%m/%d/%Y')

    # Define query parameters to fetch records from 10 days ago
    params = {
        '$limit': 999,
        '$offset': 0,
        '$where': f"issue_date = '{ten_days_ago_str}'"
    }

    # Prepare the CSV content in an in-memory string buffer
    output_buffer = StringIO()
    fieldnames = [
        'plate', 'state', 'license_type', 'summons_number', 'issue_date',
        'violation_time', 'violation', 'judgment_entry_date', 'fine_amount',
        'penalty_amount', 'interest_amount', 'reduction_amount',
        'payment_amount', 'amount_due', 'precinct', 'county',
        'issuing_agency', 'summons_image_url'
    ]
    writer = csv.DictWriter(output_buffer, fieldnames=fieldnames)
    writer.writeheader()

    offset = 0
    limit = 999

    while True:
        # Update the offset for pagination
        params['$offset'] = offset

        # Make the GET request
        response = requests.get(url, params=params)

        if response.status_code == 200:
            data = response.json()

            # If no data is returned, break the loop
            if not data:
                break

            # Write each row to the buffer
            for row in data:
                summons_image_url = row.get('summons_image', {}).get('url', '')

                writer.writerow({
                    'plate': row.get('plate', ''),
                    'state': row.get('state', ''),
                    'license_type': row.get('license_type', ''),
                    'summons_number': row.get('summons_number', ''),
                    'issue_date': row.get('issue_date', ''),
                    'violation_time': format_violation_time(row.get('violation_time', '')),
                    'violation': row.get('violation', ''),
                    'judgment_entry_date': row.get('judgment_entry_date', ''),
                    'fine_amount': row.get('fine_amount', ''),
                    'penalty_amount': row.get('penalty_amount', ''),
                    'interest_amount': row.get('interest_amount', ''),
                    'reduction_amount': row.get('reduction_amount', ''),
                    'payment_amount': row.get('payment_amount', ''),
                    'amount_due': row.get('amount_due', ''),
                    'precinct': row.get('precinct', ''),
                    'county': row.get('county', ''),
                    'issuing_agency': row.get('issuing_agency', ''),
                    'summons_image_url': summons_image_url
                })

            # Increment the offset for the next page
            offset += limit

        else:
            print(f"Failed to fetch data. Status code: {response.status_code}")
            break

    # Define S3 key and bucket
    s3_hook = S3Hook(aws_conn_id='s3_default')
    s3_key = f'path/nyc_parking_violations_{today_str_for_filename}.csv'
    s3_bucket_name = 'bucket-name'

    # Upload the string buffer to S3
    s3_hook.load_string(
        output_buffer.getvalue(),
        key=s3_key,
        bucket_name=s3_bucket_name,
        replace=True
    )

    print(f"Data successfully uploaded to S3 bucket '{s3_bucket_name}' with key '{s3_key}'")
