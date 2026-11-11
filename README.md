# AWS Weather Data Engineering Pipeline

An end-to-end data ingestion, database migration, and analytics project built using the OpenWeather API and AWS services.

## Project overview

The pipeline collects current weather observations for Hyderabad, Bengaluru, Chennai, Mumbai, and Delhi. It buffers incoming records through Amazon SQS, stores them in an Amazon RDS MySQL database, replicates the data to PostgreSQL using AWS Database Migration Service (DMS), and uses AWS Glue to incrementally write Parquet data to Amazon S3 for analysis with Amazon Athena.

## Data workflow

1. An AWS Lambda function collects weather data from the OpenWeather API.
2. The collector sends normalized records to an Amazon SQS queue.
3. A second Lambda function consumes queue messages and inserts observations into MySQL.
4. AWS DMS replicates the MySQL data to PostgreSQL using full load and change data capture (CDC).
5. An AWS Glue PySpark job reads new PostgreSQL records using an ID watermark and writes Parquet files to S3.
6. An AWS Glue Crawler updates the Data Catalog, and Athena is used to query the resulting dataset.

## Repository contents

- `lambda/weather-api-collector/` — collects and normalizes weather data and sends it to SQS.
- `lambda/weather-api-to-mysql/` — consumes SQS messages and inserts observations into MySQL.
- `glue/` — incremental PostgreSQL-to-S3 extraction job.
- `database/` — MySQL and PostgreSQL table definitions.
- `athena/` — sample analytical SQL queries.
- `.env.example` — example configuration keys with placeholder values.

## AWS services used

Amazon EventBridge Scheduler, AWS Lambda, Amazon SQS, AWS Secrets Manager, Amazon RDS for MySQL, AWS Database Migration Service, Amazon RDS for PostgreSQL, AWS Glue, Amazon S3, AWS Glue Data Catalog, Amazon Athena, AWS Identity and Access Management (IAM), and Amazon CloudWatch.

## Configuration notes

The collector Lambda expects the `SQS_QUEUE_URL` and `OPENWEATHER_SECRET_NAME` environment variables. The configured Secrets Manager secret should contain an `api_key` field.

The MySQL consumer expects `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`, and optionally `DB_CONNECT_TIMEOUT`. It requires the PyMySQL dependency, which is listed in `lambda/weather-api-to-mysql/requirements.txt`.

The Glue job accepts `CONNECTION_NAME`, `SOURCE_TABLE`, `S3_BUCKET`, `OUTPUT_PREFIX`, and `WATERMARK_KEY` job arguments. Configure the JDBC connection, IAM permissions, and S3 locations in your own AWS environment.

## Database and query files

Run the appropriate SQL script to create the source or target table:

- `database/mysql_schema.sql`
- `database/postgres_schema.sql`

Example Athena queries are available in `athena/analysis_queries.sql`.

## Local development

Create and activate a Python virtual environment, then install the consumer Lambda dependency:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r lambda/weather-api-to-mysql/requirements.txt
```

AWS Glue scripts require the AWS Glue runtime and are not intended to run as ordinary local Python scripts.
