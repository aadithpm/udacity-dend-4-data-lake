# Data Lake for Sparkify

The goal of this project is to build a data lake for Sparkify:

* Data from the Million Songs dataset that consists of song and artist data
* Logs generated with this dataset and the event generator

The ELT is built with Python. The data is pulled from S3 buckets, transformed and loaded with PySpark. Testing the Spark ELT on a small subset of the data was done locally in Spark Local while a Spark cluster with AWS ElasticMapReduce was used for the complete dataset. All data was saved to an S3 bucket in parquet. 

This documentation is split up into these following sections:

1. Table design and schemas
2. Data extraction and transformation
3. Explanation of files in the repository
4. Running the scripts

## 1. Table Design and Schemas

![ER Diagram](https://i.imgur.com/8zYZVo3.png)

## 2. Data extraction and transformation

Our song and log data JSONs are stored in two separate S3 buckets. In fact, we use 4 different buckets in your project: 2 song JSON buckets and 2 log JSON buckets. Each of the song and log buckets contain a small subset of the dataset for testing. We load these JSONs with Spark and use Spark SQL to build individual queries and construct views. After we construct our views with required transformations, we partition the data by specified attributes, if any and write it back to a specified S3 bucket as parquet.


## 3. Explanation of files in the repository


* `sparkify-test.ipynb`: Jupyter notebook that acts as a testing environment to check data integrity, AWS credentials, connection to our cluster and verify the correctness of our pipeline. Tests all our Spark scripts on a small subset of the data

* `etl.py`: Use Spark to extract, load and transform the big song and log dataset on Udacity's S3 bucket and save it as parquet to an output path, also on a S3 bucket.

## 4. Running the scripts

To run the scripts:

**Inside a terminal:**

```
python etl.py
```

It is technically possible to run the ELT script locally with Spark local mode. However, the execution time for this will be extremely long. As such, it is recommended that the final ELT pipeline be run on a Spark cluster. For local testing and messing around, the Jupyter notebook `sparkify-test` should suffice.

----
