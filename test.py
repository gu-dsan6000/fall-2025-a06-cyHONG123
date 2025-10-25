from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, to_timestamp
from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import to_timestamp
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

def main():
    spark = (
        SparkSession.builder
        .appName("Problem1_DailySummaries_Cluster")
        # Memory / cores (tune per cluster)
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.executor.cores", "2")
        .config("spark.cores.max", "6")
        # S3A (only needed if reading s3a://)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider")
        # Perf
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.ansi.enabled", "false") 

        .getOrCreate()
    )

    # root = "s3a://your-bucket/your-prefix"  # if on S3
    root = "data/sample"  # local

    logs_df = (
        spark.read.format("text")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.log")
        .load(root)
        .withColumn("source_path", F.input_file_name())
    )

    # Parse fields
    logs_parsed = (
        logs_df
        .select(
            regexp_extract('value', r'^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', 1).alias('ts_str'),
            regexp_extract('value', r'\b(INFO|WARN|ERROR|DEBUG)\b', 1).alias('log_level'),
            regexp_extract('value', r'\b(?:INFO|WARN|ERROR|DEBUG)\b\s+([^:]+):', 1).alias('component'),
            col('value').alias('message'),
            col('source_path')
        )
        .filter(F.col("log_level") != "")
        .withColumn("timestamp", to_timestamp("ts_str", "yy/MM/dd HH:mm:ss"))
        .drop("ts_str")
    )

    # Extract from file path
    df = logs_df.select(regexp_extract('value', r'^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', 1).alias('ts_str')).withColumn('file_path', input_file_name())
    df = df.withColumn('timestamp',
    to_timestamp('ts_str', 'yy/MM/dd HH:mm:ss')).drop("ts_str")
    df = df.withColumn('application_id',
        regexp_extract('file_path', r'application_(\d+_\d+)', 0))
    df = df.withColumn('container_id',
        regexp_extract('file_path', r'(container_\d+_\d+_\d+_\d+)', 1))
    df = df.withColumn('app_number', F.element_at(F.split(col('application_id'), '_'), 3))
    df = df.withColumn('cluster_id', F.element_at(F.split(col('application_id'), '_'), 2))

    #df.select('app_number').show(10, truncate=False)
    df_ts = (
        df.groupby(F.col("application_id")).agg(
            F.first("cluster_id", ignorenulls=True).alias("cluster_id"),
            F.first("app_number", ignorenulls=True).alias("app_number"),
            F.max("timestamp").alias("end_time"),
            F.min("timestamp").alias("start_time")
            )
        .select(col("cluster_id"), col("application_id"), col("app_number"), col("start_time"), col("end_time"))
    )
    df_ts.toPandas().to_csv("data/output/problem2_timeline.csv", index=False)
    #df_ts.show(5)
    df_summary = (
        df.groupby(F.col("cluster_id")).agg(
            F.countDistinct("app_number").alias("num_applications"),
            F.max("timestamp").alias("cluster_last_app"),
            F.min("timestamp").alias("cluster_first_app")
            )
        .select(col("cluster_id"), col("num_applications"), col("cluster_first_app"), col("cluster_last_app"))
    )
    #df_summary.show(5)
    df_summary.toPandas().to_csv("data/output/problem2_cluster_summary.csv", index=False)

    tc = df_summary.count()
    lltc = df_ts.count()
    
    p3 = f"""Total unique clusters: {tc}
    Total applications: {lltc}
    Average applications per cluster: {lltc/tc}

    Most heavily used clusters:
    """
    top3 = (
    df_summary
    .orderBy(F.desc("num_applications"))
    .select("cluster_id", "num_applications")
    .take(3)
    )
    for r in top3:
        lvl, cnt = r["cluster_id"], r["num_applications"]
        p3+=f"  Cluster {lvl}: {cnt} applications\n"
    with open("data/output/problem2_stats.txt", "w", encoding="utf-8") as f:
        f.write(p3)

    summary_sort = (
    df_summary
    .orderBy(F.desc("num_applications"))
    .select("cluster_id", "num_applications")
    ).toPandas()
    palette = dict(zip(
    summary_sort["cluster_id"],
    sns.color_palette("tab20", n_colors=len(summary_sort))
    ))
    plt.figure(figsize=(12, 6))
    ax = sns.barplot(
        data=summary_sort,
        x="cluster_id",
        y="num_applications",
        palette=palette,
        dodge=False
    )
    for container in ax.containers:
        ax.bar_label(container, padding=3, fontsize=10)
    ax.set_title("Number of Applications per Cluster", fontsize=14)
    ax.set_xlabel("Cluster ID")
    ax.set_ylabel("Applications")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()

    plt.savefig("data/output/problem2_bar_chart.png", dpi=150)

    lc = summary_sort["cluster_id"][0]
    print(lc)
    df_lc = df_ts.filter(F.col("cluster_id")==lc).withColumn("duration", F.col("end_time").cast("long") - F.col("start_time").cast("long")).toPandas()
    x = df_lc["duration"].astype(float)
    n = len(x)
    bins = np.logspace(np.log10(x.min()), np.log10(x.max()), 50)
    plt.figure(figsize=(10, 5))
    sns.histplot(
        x = x,
        bins=bins,
        stat="density",
        kde=True,
    )
    ax.set_xscale("log")
    ax.set_xlabel("Duration logscale")
    ax.set_ylabel("Density")
    ax.set_title(f"Application Duration Distribution for cluster {lc} count (n={n})")
    plt.tight_layout()
    plt.savefig("data/output/problem2_density_plot.png", dpi=150)

if __name__ == "__main__":
    main()
