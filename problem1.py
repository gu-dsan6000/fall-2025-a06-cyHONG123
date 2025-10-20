from pyspark.sql import functions as F, types as T
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.functions import regexp_extract, col
import os
import subprocess
import sys
import time
import logging
from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,  # Set the log level to INFO
    # Define log message format
    format="%(asctime)s,p%(process)s,{%(filename)s:%(lineno)d},%(levelname)s,%(message)s",
)

logger = logging.getLogger(__name__)


def create_spark_session(master_url):
    """Create a Spark session optimized for cluster execution."""

    spark = (
        SparkSession.builder
        .appName("Problem1_DailySummaries_Cluster")

        # Cluster Configuration
        .master(master_url)  # Connect to Spark cluster

        # Memory Configuration
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .config("spark.driver.maxResultSize", "2g")

        # Executor Configuration
        .config("spark.executor.cores", "2")
        .config("spark.cores.max", "6")  # Use all available cores across cluster
        # S3 Configuration - Use S3A for AWS S3 access
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider")

        # Performance settings for cluster execution
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

        # Serialization
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        # Arrow optimization for Pandas conversion
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")

        .master(master_url).getOrCreate()
    )

    logger.info("Spark session created successfully for cluster execution")
    return spark

def solve_problem1(spark):
    root = "s3a://ch1492-assignment-spark-cluster-logs"
    logs_df = (
        spark.read.format("text")
            .option("recursiveFileLookup", "true")
            .option("pathGlobFilter", "*.log")
            .load(root)
            .withColumn("source_path", F.input_file_name())
    )

    logs_parsed = logs_df.select(
        regexp_extract('value', r'^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', 1).alias('timestamp'),
        regexp_extract('value', r'(INFO|WARN|ERROR|DEBUG)', 1).alias('log_level'),
        regexp_extract('value', r'(INFO|WARN|ERROR|DEBUG)\s+([^:]+):', 2).alias('component'),
        col('value').alias('message')
    ).filter(F.col("log_level") != "")

    #p1.1
    logs_parsed.groupBy("log_level").count().orderBy(F.desc("count")).toPandas().to_csv("data/output/problem1_counts.csv", index=False)

    #p1.2
    import csv
    logs_parsed.select(F.col("message").alias("log_entry"), F.col("log_level")).orderBy(F.rand()).limit(10).toPandas().to_csv("data/output/problem1_sample.csv", index=False,quoting=csv.QUOTE_NONE, escapechar="\\")

    #p1.3
    tc = logs_parsed.select(F.col("log_level")).count()
    lltc = logs_parsed.select(F.col("log_level")).filter(F.col("log_level")!="").count()
    uni_c = logs_parsed.select(F.col("log_level")).distinct().count()
    gb_ll = logs_parsed.groupby(F.col("log_level")).agg(F.count("*").alias("count")).withColumn("pct", F.col("count") / F.lit(lltc) * 100.0).collect()
    p3 = f"""Total log lines processed: {tc}
    Total lines with log levels: {lltc}
    Unique log levels found: {uni_c}

    Log level distribution:
    """
    for r in gb_ll:
        lvl, cnt, p = r["log_level"], r["count"], r["pct"]
        p3+=f"  {lvl}: {cnt:>10,} ({p:5.2f}%)\n"
    with open("data/output/problem1_summary.txt", "w", encoding="utf-8") as f:
        f.write(p3)

def main():
    if len(sys.argv) > 1:
        master_url = sys.argv[1]
    else:
        # Try to get from environment variable
        master_private_ip = os.getenv("MASTER_PRIVATE_IP")
        if master_private_ip:
            master_url = f"spark://{master_private_ip}:7077"
        else:
            print("❌ Error: Master URL not provided")
            print("Usage: python nyc_tlc_problem1_cluster.py spark://MASTER_IP:7077")
            print("   or: export MASTER_PRIVATE_IP=xxx.xxx.xxx.xxx")
            return 1

    print(f"Connecting to Spark Master at: {master_url}")
    logger.info(f"Using Spark master URL: {master_url}")

    spark = create_spark_session(master_url)
    solve_problem1(spark)

if __name__ == "__main__":
    sys.exit(main())
