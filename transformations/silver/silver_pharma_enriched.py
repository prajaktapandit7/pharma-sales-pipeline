from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="silver_pharma_enriched",
    comment="Silver layer: Pharma sales enriched with product and time dimensions"
)
def silver_pharma_enriched():
    """
    Enrich bronze sales data with product and time dimensions.
    Batch read from bronze_pharma_raw (Auto Loader tracks new files).
    Optimized for scheduled pipeline runs (weekly/daily).
    Includes audit timestamps.
    """
    # Read from bronze tables (all batch reads for scheduled pipeline)
    raw = spark.read.table("bronze_pharma_raw")
    product_dim = spark.read.table("bronze_product_dim")
    time_dim = spark.read.table("bronze_time_dim")
    
    # Join with product dimension to get prod_nm, mkt_id, mkt_nm
    enriched = raw.join(
        product_dim,
        on="prod_id",
        how="left"
    )
    
    # Join with time dimension where datum is between start and end dates
    enriched = enriched.join(
        time_dim,
        on=(F.col("datum") >= F.col("strt_dt")) & (F.col("datum") <= F.col("end_dt")),
        how="left"
    )
    
    # Select relevant columns and add processing timestamp
    return enriched.select(
        F.col("datum"),
        F.col("Year"),
        F.col("Month"),
        F.col("Hour"),
        F.col("weekday_name"),
        F.col("prod_id"),
        F.col("prod_nm"),
        F.col("mkt_id"),
        F.col("mkt_nm"),
        F.col("tb_id"),
        F.col("tb_nm"),
        F.col("sales"),
        F.col("_source_file"),
        raw["_loaded_at"].alias("_loaded_at"),  # Explicitly use raw table's timestamp
        F.current_timestamp().alias("_processed_at")
    )
