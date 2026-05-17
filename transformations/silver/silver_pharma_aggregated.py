from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dp.materialized_view(
    name="silver_pharma_aggregated",
    comment="Silver layer: Aggregated pharma sales with market volume and market share calculations",
    cluster_by=["tb_id", "mkt_id", "prod_id"]
)
def silver_pharma_aggregated():
    """
    Aggregate sales by time bucket, product, and market.
    Calculate market volume and market share percentage.
    Liquid clustering on tb_id, mkt_id, prod_id for time/market/product filtering.
    Includes _loaded_at timestamp for audit trail.
    """
    # Read enriched data (batch read for aggregation)
    enriched = spark.read.table("silver_pharma_enriched")
    
    # Aggregate product sales by time bucket, product, and market
    product_agg = enriched.groupBy(
        "tb_id",
        "tb_nm",
        "prod_id",
        "prod_nm",
        "mkt_id",
        "mkt_nm"
    ).agg(
        F.sum("sales").alias("prod_volume")
    )
    
    # Calculate market volume (sum of all products in each market and time bucket)
    market_window = Window.partitionBy("tb_id", "mkt_id")
    
    with_market_vol = product_agg.withColumn(
        "market_volume",
        F.sum("prod_volume").over(market_window)
    )
    
    # Calculate market share and add audit timestamp
    return with_market_vol.withColumn(
        "market_share",
        F.when(F.col("market_volume") > 0, 
               F.col("prod_volume") / F.col("market_volume") * 100)
         .otherwise(0)
    ).select(
        "tb_id",
        "tb_nm",
        "mkt_id",
        "mkt_nm",
        "prod_id",
        "prod_nm",
        "prod_volume",
        "market_volume",
        F.round("market_share", 2).alias("market_share_pct"),
        F.current_timestamp().alias("_loaded_at")
    )
