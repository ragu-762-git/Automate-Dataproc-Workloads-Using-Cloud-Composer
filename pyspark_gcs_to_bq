"""
BigQuery I/O PySpark Demo — GCS—> Aggregate data —> write results to BigQuery
gs://gcpdatateeng-demos/scripts/pyspark_gcs_to_bq.py
"""
# Import required modules and packages
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import sum

# Create Spark session
spark = SparkSession \
    .builder \
    .master('yarn') \
    .appName('spark-bigquery-demo') \
    .getOrCreate()

# Define temporary GCS bucket for Dataproc to write it's process data
bucket='dataproc-airflow-demos'
spark.conf.set('temporaryGcsBucket', bucket)

# Read data into Spark dataframe from CSV file available in GCS bucket
df=spark.read.option("header",True).csv('gs://dataproc-airflow-demos/greenhouse_dtls.csv')

# Select limited columns and cast string type column to double
req_df=df.select(col('year'),col('anzsic_descriptor'),col('variable'),col('source'),col('data_value').cast('double'))

# Group by list of columns and SUM data_value
req_df=req_df.groupBy('year','anzsic_descriptor','variable','source').agg(sum('data_value').alias('sum_qty')).sort('year')

# Writing the data to BigQuery
req_df.write.format('bigquery').option('table','dataproc_airflow_demo.greenhouse_dtls_aggr_output').option('createDisposition','CREATE_IF_NEEDED').save()

# Stop the Spark session
spark.stop()
