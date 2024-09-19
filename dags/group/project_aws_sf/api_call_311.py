from airflow.decorators import task
import requests
import csv
from datetime import datetime, timedelta
from airflow.hooks.S3_hook import S3Hook
from io import StringIO

@task
def api_call_311():
    url = 'https://data.cityofnewyork.us/resource/erm2-nwe9.json'

    # Get today's date and the date two days ago
    today = datetime.now()
    two_days_ago = today - timedelta(days=2)

    # Format today's date for the filename: MM/DD/YYYY (no slashes in the filename)
    today_str_for_filename = today.strftime('%m%d%Y')

    # Format the dates as ISO 8601 (floating timestamp) for the API request
    today_str = today.isoformat()
    two_days_ago_str = two_days_ago.isoformat()

    # Initialize pagination parameters
    limit = 999
    offset = 0

    # Prepare the CSV content in an in-memory string buffer
    output_buffer = StringIO()
    fieldnames = [
        'unique_key', 'created_date', 'closed_date', 'agency', 'agency_name', 
        'complaint_type', 'descriptor', 'location_type', 'incident_zip', 'incident_address',
        'street_name', 'cross_street_1', 'cross_street_2', 'intersection_street_1', 
        'intersection_street_2', 'address_type', 'city', 'landmark', 'facility_type',
        'status', 'due_date', 'resolution_description', 'resolution_action_updated_date',
        'community_board', 'bbl', 'borough', 'x_coordinate_state_plane', 
        'y_coordinate_state_plane', 'open_data_channel_type', 'park_facility_name', 
        'park_borough', 'vehicle_type', 'taxi_company_borough', 'taxi_pick_up_location', 
        'bridge_highway_name', 'bridge_highway_direction', 'road_ramp', 
        'bridge_highway_segment', 'latitude', 'longitude', 'location'
    ]
    writer = csv.DictWriter(output_buffer, fieldnames=fieldnames)
    writer.writeheader()

    # Continue fetching data using pagination until no more data is returned
    while True:
        # Define query parameters to fetch records using limit and offset
        params = {
            '$limit': limit,
            '$offset': offset,
            '$where': f"created_date >= '{two_days_ago_str}' AND created_date <= '{today_str}'"
        }

        # Make the GET request
        response = requests.get(url, params=params)

        if response.status_code == 200:
            data = response.json()

            # If no data is returned, break out of the loop
            if not data:
                break

            # Write each row to the CSV buffer
            for row in data:
                writer.writerow({
                    'unique_key': row.get('unique_key', ''),
                    'created_date': row.get('created_date', ''),
                    'closed_date': row.get('closed_date', ''),
                    'agency': row.get('agency', ''),
                    'agency_name': row.get('agency_name', ''),
                    'complaint_type': row.get('complaint_type', ''),
                    'descriptor': row.get('descriptor', ''),
                    'location_type': row.get('location_type', ''),
                    'incident_zip': row.get('incident_zip', ''),
                    'incident_address': row.get('incident_address', ''),
                    'street_name': row.get('street_name', ''),
                    'cross_street_1': row.get('cross_street_1', ''),
                    'cross_street_2': row.get('cross_street_2', ''),
                    'intersection_street_1': row.get('intersection_street_1', ''),
                    'intersection_street_2': row.get('intersection_street_2', ''),
                    'address_type': row.get('address_type', ''),
                    'city': row.get('city', ''),
                    'landmark': row.get('landmark', ''),
                    'facility_type': row.get('facility_type', ''),
                    'status': row.get('status', ''),
                    'due_date': row.get('due_date', ''),
                    'resolution_description': row.get('resolution_description', ''),
                    'resolution_action_updated_date': row.get('resolution_action_updated_date', ''),
                    'community_board': row.get('community_board', ''),
                    'bbl': row.get('bbl', ''),
                    'borough': row.get('borough', ''),
                    'x_coordinate_state_plane': row.get('x_coordinate_state_plane', ''),
                    'y_coordinate_state_plane': row.get('y_coordinate_state_plane', ''),
                    'open_data_channel_type': row.get('open_data_channel_type', ''),
                    'park_facility_name': row.get('park_facility_name', ''),
                    'park_borough': row.get('park_borough', ''),
                    'vehicle_type': row.get('vehicle_type', ''),
                    'taxi_company_borough': row.get('taxi_company_borough', ''),
                    'taxi_pick_up_location': row.get('taxi_pick_up_location', ''),
                    'bridge_highway_name': row.get('bridge_highway_name', ''),
                    'bridge_highway_direction': row.get('bridge_highway_direction', ''),
                    'road_ramp': row.get('road_ramp', ''),
                    'bridge_highway_segment': row.get('bridge_highway_segment', ''),
                    'latitude': row.get('latitude', ''),
                    'longitude': row.get('longitude', ''),
                    'location': row.get('location', '')
                })

            # Increment the offset for the next page of data
            offset += limit

        else:
            print(f"Failed to fetch data. Status code: {response.status_code}")
            break

    # Define S3 key and bucket using the formatted date for the filename
    s3_hook = S3Hook(aws_conn_id='s3_default')
    s3_key = f'path/nyc_311_requests_{today_str_for_filename}.csv'
    s3_bucket_name = 'bucket-name'

    # Upload the string buffer to S3
    s3_hook.load_string(
        output_buffer.getvalue(),
        key=s3_key,
        bucket_name=s3_bucket_name,
        replace=True
    )

    print(f"Data successfully uploaded to S3 bucket '{s3_bucket_name}' with key '{s3_key}'")
