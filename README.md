# AWS Streaming Data Architecture

This project demonstrates an AWS-based streaming data architecture designed using draw.io.

The architecture shows how data can be collected from an external source, processed through AWS streaming and serverless services, transformed and stored in Amazon S3, and finally queried and visualised for analytics.

### Editable Diagram

The original draw.io file is included in this repository for viewing and editing:

[Open the draw.io source file](AWS_Entity_Project.drawio)

## Architecture Overview

The architecture is divided into four main stages:

### 1. Data Ingestion

The ingestion process begins with an AWS Lambda function that communicates with an external data source through an API.

CloudWatch Event Scheduler can trigger the Lambda function at scheduled intervals.

AWS Systems Manager and Amazon DynamoDB are also used to support the ingestion process.

The Lambda producer sends the collected records to Amazon Kinesis Data Streams, where another Lambda function consumes the streaming data.

### 2. AI / Inference Processing

The consumed data is passed to an AWS Step Functions workflow.

The workflow coordinates different AWS services such as:

- Amazon Rekognition
- Amazon Translate
- Amazon Comprehend

These services can be used for image recognition, language translation and natural language processing.

Amazon S3 is used to store raw text or data required for modelling and processing.

### 3. Streaming and ETL

After the inference process, events are passed through Amazon EventBridge.

The processed events are streamed through Amazon Kinesis and sent to AWS Glue.

AWS Glue performs ETL processing before storing the processed inference data in Amazon S3.

### 4. Analytics

The processed data stored in Amazon S3 can be prepared using AWS Glue and queried using Amazon Athena.

Amazon QuickSight provides the visualisation layer where users can explore dashboards and analytical results.

## Data Flow

The main processing flow is:

Lambda Producer → Kinesis Data Stream → Lambda Consumer → Step Functions → EventBridge → Kinesis → AWS Glue → Amazon S3

The analytics layer uses:

Amazon S3 → AWS Glue / Athena → Amazon QuickSight → User

## AWS Services Used

- AWS Lambda
- Amazon Kinesis Data Streams
- AWS Step Functions
- Amazon Rekognition
- Amazon Translate
- Amazon Comprehend
- Amazon EventBridge
- AWS Glue
- Amazon S3
- Amazon Athena
- Amazon QuickSight
- Amazon DynamoDB
- AWS Systems Manager
- Amazon CloudWatch

## Tools Used

- AWS Architecture Icons
- draw.io
- GitHub

## What I Learned

Through this project, I developed a better understanding of how AWS services can work together to build a streaming data pipeline.

I learned how data can move from ingestion through real-time streaming, AI processing, ETL and storage before being queried and visualised for end users.

I also gained experience creating and organising AWS architecture diagrams using draw.io.
