from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="bronze_product_dim",
    comment="Bronze layer: Product master dimension with product and market information"
)
def bronze_product_dim():
    """
    Product master dimension mapping products to markets.
    Static reference data with audit timestamp.
    """
    # Define product master data
    products_data = [
        ("M01AB", "Acetic acid derivatives and related substances", 1001, "Anti-inflammatory and antirheumatic products, non-steroids"),
        ("M01AE", "Propionic acid derivatives", 1001, "Anti-inflammatory and antirheumatic products, non-steroids"),
        ("N02BA", "Salicylic acid and derivatives", 1002, "Other analgesics and antipyretics"),
        ("N02BE", "Pyrazolones and Anilides", 1002, "Other analgesics and antipyretics"),
        ("N05B", "Anxiolytic drugs", 1003, "Psycholeptics drugs"),
        ("N05C", "Hypnotics and sedatives drugs", 1003, "Psycholeptics drugs"),
        ("R03", "Drugs for obstructive airway diseases", 1004, "Drugs for obstructive airway diseases"),
        ("R06", "Antihistamines for systemic use", 1005, "Antihistamines for systemic use")
    ]
    
    df = spark.createDataFrame(
        products_data,
        ["prod_id", "prod_nm", "mkt_id", "mkt_nm"]
    )
    
    # Add audit timestamp
    return df.withColumn("_loaded_at", F.current_timestamp())
