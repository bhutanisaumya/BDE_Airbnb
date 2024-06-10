### ELT Data Pipeline with dbt
## Overview
This project demonstrates an ELT (Extract, Load, Transform) data pipeline using dbt (Data Build Tool) to process and analyze Airbnb and Census data. dbt enables the creation and maintenance of clean, accurate, and well-documented data models for analysis and reporting.

The pipeline integrates Apache Airflow for workflow orchestration, PostgreSQL for local storage, and dbt for data transformation. Data is extracted from Airflow buckets, loaded into PostgreSQL, and transformed using dbt to apply business logic and create modular transformations. This setup ensures efficient data processing and comprehensive documentation for advanced analytics.

### Setup
#### GCP Account Setup
1. Sign up for a GCP account.
2. Enable the Cloud Composer API.
3. Create a Cloud Composer environment (Composer 1).
4. Add PyPI packages (pandas and apache-airflow-providers-postgres) to the environment.
5. Configure Airflow.
6. Create a PostgreSQL instance in Google Cloud SQL and retrieve its public IP address.

#### DBeaver PostgreSQL Connection Setup
1. Download and install DBeaver Community Edition.
2. Select "PostgreSQL" as the database type.
3. Add the connection details and test the connection using the PostgreSQL instance's public IP.

### Using the starter project
To run the project, use the following commands:
- dbt run
- dbt test

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
