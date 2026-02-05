"""
Data Quality Processor using PySpark
Processes streaming data and performs quality checks on GCP/Databricks
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, when, count, avg, stddev
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataQualityProcessor:
    def __init__(self, app_name="DataQualityPipeline"):
        """Initialize Spark session for data quality processing"""
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
            .getOrCreate()
        
        logger.info(f"Initialized {app_name} Spark session")
    
    def define_schema(self):
        """Define schema for incoming streaming data"""
        return StructType([
            StructField("id", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("sensor_id", StringType(), False),
            StructField("value", DoubleType(), False),
            StructField("location", StringType(), True),
            StructField("metadata", StringType(), True)
        ])
    
    def read_from_gcs(self, bucket_path):
        """Read streaming data from Google Cloud Storage"""
        schema = self.define_schema()
        
        df = self.spark.readStream \
            .format("json") \
            .schema(schema) \
            .option("maxFilesPerTrigger", 1) \
            .load(bucket_path)
        
        logger.info(f"Reading streaming data from GCS: {bucket_path}")
        return df
    
    def apply_quality_checks(self, df):
        """Apply data quality rules and validations"""
        # Add data quality flags
        quality_df = df.withColumn(
            "is_valid",
            when(
                (col("value").isNotNull()) &
                (col("sensor_id").isNotNull()) &
                (col("value") >= 0) &
                (col("value") <= 1000), # Business rule: value range
                True
            ).otherwise(False)
        ).withColumn(
            "quality_check_timestamp",
            current_timestamp()
        ).withColumn(
            "anomaly_score",
            when(col("value") > 800, 3.0)  # High anomaly
            .when(col("value") > 600, 2.0)  # Medium anomaly
            .when(col("value") > 400, 1.0)  # Low anomaly
            .otherwise(0.0)  # Normal
        )
        
        logger.info("Applied data quality checks")
        return quality_df
    
    def detect_anomalies(self, df):
        """Detect statistical anomalies using z-score method"""
        stats = df.groupBy("sensor_id").agg(
            avg("value").alias("mean_value"),
            stddev("value").alias("stddev_value")
        )
        
        anomaly_df = df.join(stats, "sensor_id").withColumn(
            "z_score",
            (col("value") - col("mean_value")) / col("stddev_value")
        ).withColumn(
            "is_anomaly",
            when(abs(col("z_score")) > 3, True).otherwise(False)
        )
        
        logger.info("Anomaly detection completed")
        return anomaly_df
    
    def write_to_databricks_delta(self, df, table_name, checkpoint_path):
        """Write processed data to Databricks Delta Lake"""
        query = df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", checkpoint_path) \
            .table(table_name)
        
        logger.info(f"Writing to Delta table: {table_name}")
        return query
    
    def write_to_gcs_parquet(self, df, output_path, checkpoint_path):
        """Write processed data to GCS in Parquet format"""
        query = df.writeStream \
            .format("parquet") \
            .outputMode("append") \
            .option("path", output_path) \
            .option("checkpointLocation", checkpoint_path) \
            .start()
        
        logger.info(f"Writing to GCS Parquet: {output_path}")
        return query
    
    def run_quality_pipeline(self, input_path, output_path, checkpoint_path):
        """Execute end-to-end data quality pipeline"""
        logger.info("Starting data quality pipeline")
        
        # Read streaming data
        raw_df = self.read_from_gcs(input_path)
        
        # Apply quality checks
        quality_df = self.apply_quality_checks(raw_df)
        
        # Write to output
        query = self.write_to_gcs_parquet(quality_df, output_path, checkpoint_path)
        
        logger.info("Data quality pipeline started successfully")
        return query

if __name__ == "__main__":
    # Example usage
    processor = DataQualityProcessor()
    
    input_path = "gs://your-bucket/raw-data/"
    output_path = "gs://your-bucket/quality-checked-data/"
    checkpoint_path = "gs://your-bucket/checkpoints/quality-pipeline/"
    
    query = processor.run_quality_pipeline(input_path, output_path, checkpoint_path)
    query.awaitTermination()
