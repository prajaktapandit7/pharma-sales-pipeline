from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dp.materialized_view(
    name="gold_pharma_sales",
    comment="Gold layer: Pharma sales with market share rankings and YoY growth rates",
    cluster_by=["time_bucket_id", "market_id", "product_id"]
)
def gold_pharma_sales():
    """
    Final reporting table with:
    - Product rankings by market share within each year/market
    - Year-over-year sales growth rate calculation
    - Liquid clustering on time_bucket_id, market_id, and product_id for optimal query performance
    """
    # Read from silver aggregated table
    agg = spark.read.table("silver_pharma_aggregated")
    
    # Add product ranking by market share within each year and market
    ranking_window = Window.partitionBy("tb_id", "mkt_id").orderBy(F.desc("market_share_pct"))
    
    with_rank = agg.withColumn(
        "product_rank",
        F.row_number().over(ranking_window)
    )
    
    # Calculate year-over-year sales growth rate
    # Get previous year's sales for the same product
    growth_window = Window.partitionBy("prod_id").orderBy("tb_id")
    
    with_growth = with_rank.withColumn(
        "prev_year_volume",
        F.lag("prod_volume", 1).over(growth_window)
    ).withColumn(
        "sales_growth_rate_pct",
        F.when(
            F.col("prev_year_volume").isNotNull() & (F.col("prev_year_volume") > 0),
            F.round(
                ((F.col("prod_volume") - F.col("prev_year_volume")) / F.col("prev_year_volume")) * 100,
                2
            )
        ).otherwise(None)
    ).drop("prev_year_volume")
    
    # Create final reporting table with clean column names and audit timestamp
    return with_growth.select(
        F.col("tb_id").alias("time_bucket_id"),
        F.col("tb_nm").alias("time_period"),
        F.col("mkt_id").alias("market_id"),
        F.col("mkt_nm").alias("market_name"),
        F.col("prod_id").alias("product_id"),
        F.col("prod_nm").alias("product_name"),
        F.col("prod_volume").alias("product_sales_volume"),
        F.col("market_volume").alias("total_market_volume"),
        F.col("market_share_pct").alias("market_share_percentage"),
        "product_rank",
        "sales_growth_rate_pct",
        F.current_timestamp().alias("_loaded_at")
    ).orderBy("time_bucket_id", "market_id", "product_rank")
