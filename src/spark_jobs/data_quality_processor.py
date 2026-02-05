"""
Data Quality Processor using PySpark
Processing streaming data and checking quality on GCP/Databricks
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, when, count, avg, stddev
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataQualityProcessor:
    def __init__(self, app_name="DataQualityPipeline"):
        """Setting up the Spark session - needed to run everything"""
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
            .getOrCreate()
        
        logger.info(f"Started {app_name} Spark session")
    
    def define_schema(self):
        """Here's what our data looks like - defining the structure"""
        return StructType([
            StructField("id", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("sensor_id", StringType(), False),
            StructField("value", DoubleType(), False),
            StructField("location", StringType(), True),
            StructField("metadata", StringType(), True)
        ])
    
    def read_from_gcs(self, bucket_path):
        """Reading data from Google Cloud Storage as it comes in"""
        schema = self.define_schema()
        
        df = self.spark.readStream \
            .format("json") \
            .schema(schema) \
            .option("maxFilesPerTrigger", 1) \
            .load(bucket_path)
        
        logger.info(f"Reading streaming data from GCS: {bucket_path}")
        return df
    
    def apply_quality_checks(self, df):
        """Checking if the data looks good - adding flags for bad data"""
        # Mark rows as valid or invalid based on our rules
        quality_df = df.withColumn(
            "is_valid",
            when(
                (col("value").isNotNull()) &
                (col("sensor_id").isNotNull()) &
                (col("value") >= 0) &
                (col("value") <= 1000), # Values should be between 0-1000
                True
            ).otherwise(False)
        ).withColumn(
            "quality_check_timestamp",
            current_timestamp()
        ).withColumn(
            "anomaly_score",
            # Scoring how weird the values are
            when(col("value") > 800, 3.0)  # Really high - potential issue
            .when(col("value") > 600, 2.0)  # Kinda high
            .when(col("value") > 400, 1.0)  # Slightly elevated
            .otherwise(0.0)  # Looks normal
        )
        
        logger.info("Applied quality checks to data")
        return quality_df
    
    def detect_anomalies(self, df):
        """Finding outliers using z-score - basically checking what's too far from normal"""
        # Calculate mean and standard deviation for each sensor
        stats = df.groupBy("sensor_id").agg(
            avg("value").alias("mean_value"),
            stddev("value").alias("stddev_value")
        )
        
        # Join with original data and calculate z-score
        anomaly_df = df.join(stats, "sensor_id").withColumn(
            "z_score",
            (col("value") - col("mean_value")) / col("stddev_value")
        ).withColumn(
            "is_anomaly",
            when(abs(col("z_score")) > 3, True).otherwise(False)  # 3 standard deviations = anomaly
        )
        
        logger.info("Anomaly detection done")
        return anomaly_df
    
    def write_to_databricks_delta(self, df, table_name, checkpoint_path):
        """Saving the processed data to Databricks Delta Lake"""
        query = df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", checkpoint_path) \
            .table(table_name)
        
        logger.info(f"Writing to Delta table: {table_name}")
        return query
    
    def write_to_gcs_parquet(self, df, output_path, checkpoint_path):
        """Saving the data to GCS in Parquet format - good for compression"""
        query = df.writeStream \
            .format("parquet") \
            .outputMode("append") \
            .option("path", output_path) \
            .option("checkpointLocation", checkpoint_path) \
            .start()
        
        logger.info(f"Writing to GCS Parquet: {output_path}")
        return query
    
    def run_quality_pipeline(self, input_path, output_path, checkpoint_path):
        """Main function that ties everything together"""
        logger.info("Starting the data quality pipeline")
        
        # Step 1: Read the incoming data
        raw_df = self.read_from_gcs(input_path)
        
        # Step 2: Run quality checks
        quality_df = self.apply_quality_checks(raw_df)
        
        # Step 3: Save the results
        query = self.write_to_gcs_parquet(quality_df, output_path, checkpoint_path)
        
        logger.info("Pipeline is now running!")
        return query

if __name__ == "__main__":
    # This runs when you execute the file directly
    processor = DataQualityProcessor()
    
    # TODO: Replace these with your actual bucket paths
    input_path = "gs://your-bucket/raw-data/"
    output_path = "gs://your-bucket/quality-checked-data/"
    checkpoint_path = "gs://your-bucket/checkpoints/quality-pipeline/"
    
    query = processor.run_quality_pipeline(input_path, output_path, checkpoint_path)
    query.awaitTermination()  # Keeps the job running until manually stopped
