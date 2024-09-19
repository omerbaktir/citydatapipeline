This repo is for the codes and files of the pipeline I prepared for a daily automation of a database containing information regarding requests and violations.

# City Data Pipeline Project

## Table of Contents

- [Introduction](#introduction)
- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Data Sources](#data-sources)
- [Pipeline Steps](#pipeline-steps)
- [Challenges Faced](#challenges-faced)
- [Future Enhancements](#future-enhancements)
- [Setup and Installation](#setup-and-installation)
- [Usage](#usage)
- [Power BI Dashboard](#power-bi-dashboard)
- [Contributing](#contributing)
- [License](#license)

## Introduction

This project demonstrates a data pipeline that automates the extraction of parking violations and 311 request data, stores it for reference, transforms it, and visualizes it using Power BI. The pipeline is orchestrated using Apache Airflow and leverages AWS S3 and Snowflake for storage and data warehousing.

## Project Overview

- **Data Retrieval**: Daily extraction of parking violations and 311 requests data from NYC Open Data APIs.
- **Storage**: Raw data is stored in AWS S3 with a specific naming convention.
- **Transformation**: Selected columns are transformed and loaded into Snowflake.
- **Visualization**: Data is visualized through a Power BI dashboard, accessible to selected users.
- **Orchestration**: Airflow manages the end-to-end workflow, ensuring daily updates and data integrity.

## Architecture

_An architecture diagram illustrating the data flow from APIs to Power BI via Airflow, S3, and Snowflake._

## Data Sources

1.  **NYC Parking Violations API**
2.  **NYC 311 Requests API**

## Pipeline Steps

1.  **Data Extraction**

    - Fetch data for the current day using the APIs.
    - Handle API limits and ensure data integrity.

2.  **Data Storage in S3**

    - Save the raw data files in AWS S3.
    - Follow the naming convention:
      - `violations.DDMMYYYY.csv`
      - `requests.DDMMYYYY.csv`

3.  **Data Transformation and Loading**

    - Use Snowflake to perform data type transformations.
    - Load selected columns into Snowflake tables.
    - Ensure SQL commands are tested locally before deployment.

4.  **Data Visualization**

    - Provide access to selected Snowflake users.
    - Refresh the Power BI dashboard to reflect the latest data.

5.  **Workflow Orchestration**

    - Airflow DAG schedules and manages the entire process.
    - Future adaptation to run Airflow on EC2 or MWAA.

## Challenges Faced

1.  **API Limits and Data Fetching**

    - Managed rate limits and ensured complete data retrieval from both APIs.
    - Implemented retry mechanisms and data validation checks.

2.  **File Naming Conventions**

    - Established strict naming patterns for consistency and easier data management.

3.  **AWS S3 and Airflow Connections**

    - Configured secure and efficient connections between Airflow and AWS S3.
    - Ensured proper permissions and access controls.

4.  **Data Loading into Snowflake**

    - Set up direct data copying from S3 to Snowflake stages.
    - Optimized the loading process for large data volumes.

5.  **Local Testing**

    - Performed local testing of APIs, connections, and SQL commands.
    - Validated transformations and data types before full-scale deployment.

6.  **Data Transformation in Snowflake**

    - Addressed data type mismatches and formatting issues.
    - Leveraged Snowflake's capabilities for efficient data processing.

## Future Enhancements

- **Airflow Deployment**

  - Migrate Airflow to AWS EC2 or Managed Workflows for Apache Airflow (MWAA) for better scalability and reliability.

- **Additional Data Sources**

  - Integrate more datasets to enrich the analysis.

- **Enhanced Visualization**

  - Implement advanced analytics and interactive features in the Power BI dashboard.

## Setup and Installation

1.  **Clone the Repository**

    bash

    Copy code

    `git clone https://github.com/omerbaktir/citydatapipeline.git`

2.  **Configure Credentials**

    - Update the configuration files in the `config/` directory with your AWS, Snowflake, and API credentials.

3.  **Set Up Airflow**

    - Install Apache Airflow.
    - Place the DAG files in the Airflow DAGs directory.
    - Configure Airflow connections for AWS and Snowflake.

4.  **Set Up Snowflake**

    - Create the necessary tables using the scripts in the `sql/` directory.
    - Ensure proper user permissions are set.

5.  **Prepare AWS S3**

    - Create an S3 bucket for storing the data files.
    - Set up IAM roles and policies for access.

6.  **Install Dependencies**

    - Install required Python packages:

      Copy code

      `pip install -r requirements.txt`

## Usage

- **Run the Airflow Scheduler**

  - Start the Airflow scheduler and webserver to begin the daily data pipeline.

- **Monitor the Pipeline**

  - Use the Airflow UI to monitor the DAG runs and troubleshoot if necessary.

- **Access Data in Snowflake**

  - Connect to Snowflake to query and analyze the transformed data.

## Power BI Dashboard

- **Accessing the Dashboard**

  - Open the `dashboard.pbix` file in Power BI Desktop.
  - Ensure connectivity to Snowflake is configured with appropriate credentials.

- **Refreshing Data**

  - Set up scheduled refreshes to keep the dashboard up-to-date with the latest data.

- **Dashboard Features**

  - Visual representations of parking violations and 311 requests.
  - Filters and slicers for date ranges and specific data segments.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any improvements or suggestions.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
