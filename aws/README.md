## Overview of Cloud Deployment in AWS

This repository contains scripts for managing a data pipeline within an AWS environment. The names in parentheses within the file names specify the cloud services where each script is executed.

### Workflow
The data pipeline workflow includes:
- **Data Ingestion**: Initial data collection.
- **Data Cleaning**: Processing and sanitizing the ingested data.
- **Data Labeling**: Tagging data, all triggered by a Spark job and visualized on the dashboard.

Additionally, the schema script is responsible for building tables for raw data.

### Folder Structure
- **`docker_image` Folder**: Contains scripts to create a virtual environment in local for executing Spark jobs.
- **`reddit_dashboard` Folder**: Includes a cron job that generates `labeled_data_final.rds`, which is then read by the Shiny script (`app.r`).

Feel free to explore each folder for further details on implementation.
