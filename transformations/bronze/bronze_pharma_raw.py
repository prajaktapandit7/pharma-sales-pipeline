from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="bronze_pharma_raw",
    comment="Bronze layer: Raw pharma sales data ingested from S3 with Auto Loader, unpivoted by product",
    cluster_by=["datum", "prod_id"]
)
def bronze_pharma_raw():
    """
    Ingest pharmaceutical sales data from S3 using Auto Loader.
    Unpivot product columns into prod_id and sales rows.
    Liquid clustering on datum and prod_id for date/product filtering.
    Includes _loaded_at timestamp and _source_file for audit trail.
    """
    # Auto Loader to read from S3 - serverless manages schema location automatically
    df = (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "csv")
          .option("cloudFiles.useIncrementalListing", "auto")
          .option("header", "true")
          .option("inferSchema", "true")
          .load("s3://amazon-l0-landing-prod/")
    )
    
    # Unpivot product columns (M01AB, M01AE, N02BA, N02BE, N05B, N05C, R03, R06)
    products = ["M01AB", "M01AE", "N02BA", "N02BE", "N05B", "N05C", "R03", "R06"]
    
    unpivoted = df.select(
        F.col("datum"),
        F.col("Year"),
        F.col("Month"),
        F.col("Hour"),
        F.col("`Weekday Name`").alias("weekday_name"),
        F.expr(f"stack({len(products)}, " + 
               ", ".join([f"'{p}', `{p}`" for p in products]) + 
               ") as (prod_id, sales)")
    ).filter(F.col("sales").isNotNull())  # Remove null sales
    
    # Add audit columns - use Unity Catalog compatible _metadata
    return unpivoted.withColumn(
        "_source_file", F.col("_metadata.file_path")
    ).withColumn(
        "_loaded_at", F.current_timestamp()
    )
