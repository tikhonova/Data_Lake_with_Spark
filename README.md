# Data Lake on AWS with Spark

The purpose of this project is to build an ETL pipeline for a Data Lake hosted on Amazon S3
. It extracts data from a S3 bucket, wrangles it in memory with Spark and writes it back to S3 for further analysis.


### Prerequisites

* Install [Python 3.x](https://www.python.org/).


## Libraries

* configparser
* datetime
* os
* pyspark.sql


## Files
* **etl.py** main script
* **dl.cfg** AWS credentials


## Running Script using Terminal
* python etl.py


## Data Model
![Song ERD](./Song_ERD.png)

This is a **start schema** data model. It has a **Fact Table -songplays-** that contains facts on played songs, such as user agent, location, session. It's equipped with **foreign keys (FK)** for **4 dimension tables** :

* **Songs** table
* **Artists** table
* **Users** table
* **Time** table

This model enables fast read queries with the minimum number of **SQL JOINs**.
