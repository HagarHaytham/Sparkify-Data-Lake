# Sparkify Data Lake

## Introduction

Sparkify is a startup with a new song streaming app. In order to keep up with the market, Sparkify needs to go to the next level which is user behavioral analytics. To achieve that as a bussiness need we need to go through the process of ETL on Data Lake. So instead of having data kept in the logs on their local machines, they uploaded them to S3 buckets to make data streaming easier, they needed their data to be organized so that they can easily have wonderful insights from it,So we used the star schema to make the analytics easier. Here we go through the ELT process with AWS EMR (Amazon Elastic Map Reduce) cluster as we
load the data from S3 buckets and process them using spark and then write the final data on s3 bucket to be accessed by the analytics team easily.

## How to run the project scripts

1. First, you need to create an AWS EMR cluster with hadoop, spark and hive services

2. Then you need to ssh to the cluster and put both files dl.cfg and etl.py under the hadoop user

3. After that you need to run the ___etl.py___ script, here is where all the work is done, logs are read from the s3 bucket and data that is needed is extracted and processed using spark and then inserted into the new s bucket.

>/usr/bin/spark-submit --master yarn ./etl.py


## Files Explaination

- ***etl.py :*** This file loads the data from the S3 bucket , processes it using spark ,then inserts the data processed to the 5 tables of the the new star schema on the new s3 bucket.
- ***dl.cfg:*** This file contains the credintionals of AWS
- ***README.md :*** The current file which contains project disscusion.


## Database Schema Design
Sparkify Database Schema is the Star Schema as it consists of one fact table (sonplays table) and four dimension tables (songs, artists, users, time). This schema is helpul as it has one to one relationships, so it simplifies the queries and makes fast aggregations.

## ELT Pipeline
As mentioned before the data source used here is the S3 bucket, So at first we ***Extract*** the events logs and song data from the S3 buckets, then we ***LOAD*** them in spark to begin processing and quering using spark sql which is the ***Tranform*** part, At the end we write the new data presented in the star schema ( one fact table anf four dimentionl tables) on the output S3 bucket.
