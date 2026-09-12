# Dataset

This project uses a synthetic Eurostar railway dataset designed to simulate operational and transactional data for an end-to-end AWS data engineering pipeline.

## Main Source Tables

- `stations`
- `trains`
- `journeys`
- `bookings`
- `payments`
- `disruptions`
- `refunds`

## Dataset Purpose

The dataset was used to demonstrate:

- Data ingestion from PostgreSQL using AWS DMS
- Raw storage in Amazon S3 Bronze layer
- Data cleaning and transformation using AWS Glue and PySpark
- Silver and Gold layer processing
- Star schema modelling for analytics
- Validation with Amazon Athena
- Analytical querying through Amazon Redshift
- Dashboard creation in Amazon QuickSight

## Data Layers

### Bronze
Raw data ingested from PostgreSQL using AWS DMS.

### Silver
Cleaned and transformed Parquet data.

### Gold
Business-ready fact and dimension tables used for analytics and reporting.
