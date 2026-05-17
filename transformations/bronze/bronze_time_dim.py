from pyspark import pipelines as dp
from pyspark.sql import functions as F
from datetime import date

@dp.materialized_view(
    name="bronze_time_dim",
    comment="Bronze layer: Time dimension for years 2014-2019"
)
def bronze_time_dim():
    """
    Time dimension defining yearly time buckets.
    Static reference data with audit timestamp.
    """
    # Define time buckets for years 2014-2019
    time_data = [
        (101, "Y1", date(2014, 1, 1), date(2014, 12, 31)),
        (102, "Y2", date(2015, 1, 1), date(2015, 12, 31)),
        (103, "Y3", date(2016, 1, 1), date(2016, 12, 31)),
        (104, "Y4", date(2017, 1, 1), date(2017, 12, 31)),
        (105, "Y5", date(2018, 1, 1), date(2018, 12, 31)),
        (106, "Y6", date(2019, 1, 1), date(2019, 12, 31))
    ]
    
    df = spark.createDataFrame(
        time_data,
        ["tb_id", "tb_nm", "strt_dt", "end_dt"]
    )
    
    # Add audit timestamp
    return df.withColumn("_loaded_at", F.current_timestamp())
