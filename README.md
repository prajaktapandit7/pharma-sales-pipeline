# Pharma Sales Pipeline - 3-Layer Medallion Architecture

## Overview
This Lakeflow Spark Declarative Pipeline implements a medallion architecture (Bronze → Silver → Gold) for pharmaceutical sales data analytics. The pipeline processes historical sales data from S3, performs dimensional enrichment, and produces business-ready analytics including market share and year-over-year growth metrics.

**Catalog**: `pharma`  
**Mode**: Serverless, Triggered (weekly schedule)

---

## Data Source & Business Goal

### Source Data
* **Location**: S3 bucket `s3://amazon-l0-landing-prod`
* **Format**: CSV files with pharmaceutical sales data
* **Base Table**: `pharma.default.table`
* **Time Range**: 2014-2019 (6 years)
* **Granularity**: Daily sales records with temporal attributes

### Product Coverage (8 Products across 5 Markets)
 Product Code | Product Name | Market ID | Market Name |
--------------|--------------|-----------|-------------|
 M01AB | Acetic acid derivatives | 1001 | Market 1001 |
 M01AE | Propionic acid derivatives | 1001 | Market 1001 |
 N02BA | Salicylic acid derivatives | 1002 | Market 1002 |
 N02BE | Pyrazolones and Anilides | 1002 | Market 1002 |
 N05B | Anxiolytic drugs | 1003 | Market 1003 |
 N05C | Hypnotics and sedatives | 1003 | Market 1003 |
 R03 | Drugs for obstructive airway diseases | 1004 | Market 1004 |
 R06 | Antihistamines | 1005 | Market 1005 |

### Business Objectives
1. **Automated Data Ingestion**: Auto-refresh when new CSV files arrive in S3 (file tracking via Auto Loader checkpoints)
2. **Dimensional Modeling**: Unpivot product columns and join with product/time dimensions
3. **Market Analytics**: Calculate market share percentage for each product within its market
4. **Trend Analysis**: Compute year-over-year sales growth rates
5. **Performance**: Liquid clustering for optimized query performance on time/market/product filters

---

## Architecture Decisions

### Streaming vs. Batch Strategy
**Decision**: Hybrid approach—streaming bronze layer with batch downstream layers

**Rationale**:
* **Bronze Layer (Streaming)**: Auto Loader tracks processed files via checkpoints, ensuring only new S3 files are read even on weekly schedule
* **Silver/Gold Layers (Batch)**: Materialized views with serverless incremental refresh—more efficient for weekly triggered runs than continuous streaming propagation
* **Benefit**: Combines file-level deduplication (Auto Loader) with compute efficiency (batch processing)

### Liquid Clustering Strategy
**Decision**: Apply liquid clustering to all high-traffic tables with multi-column filter patterns

**Tables**:
* `bronze_pharma_raw`: Clustered by `datum, prod_id` (date and product filtering)
* `silver_pharma_aggregated`: Clustered by `tb_id, mkt_id, prod_id` (time bucket, market, product)
* `gold_pharma_sales`: Clustered by `time_bucket_id, market_id, product_id` (reporting queries)

**Benefit**: Auto-optimizing clustering adapts to query patterns without manual maintenance (superior to static Z-ordering)

### Audit Trail & Data Governance
* **All Tables**: Permanent Delta tables in Unity Catalog with time travel support
* **Audit Columns**:
  * `_loaded_at`: Timestamp when data entered the layer (all tables)
  * `_source_file`: Original S3 file path (bronze raw layer)
  * `_processed_at`: Timestamp when transformations applied (silver enriched)
* **Incremental Processing**: Streaming tables append-only; materialized views use smart refresh
* **Historical Queries**: Query any historical version via `VERSION AS OF` or `TIMESTAMP AS OF`

---

## Pipeline Structure

```
pharma-sales-pipeline/
├── transformations/
│   ├── bronze/
│   │   ├── bronze_pharma_raw.py          # Streaming table (Auto Loader from S3)
│   │   ├── bronze_product_dim.py         # Product dimension (static)
│   │   └── bronze_time_dim.py            # Time bucket dimension (Y1-Y6)
│   ├── silver/
│   │   ├── silver_pharma_enriched.py     # Dimensional joins (batch)
│   │   └── silver_pharma_aggregated.py   # Market aggregations (batch)
│   └── gold/
│       └── gold_pharma_sales.py          # Market share & YoY growth (batch)
└── README.md                              # This file
```

---

## Layer Details

### Bronze Layer (`pharma.bronze`)

#### 1. `bronze_pharma_raw` (Streaming Table)
**Purpose**: Ingest raw CSV files from S3 and unpivot product columns

**Source**: Auto Loader (`cloudFiles` format, CSV, schema inference)

**Transformation**:
* Unpivots 8 product columns (M01AB, M01AE, N02BA, N02BE, N05B, N05C, R03, R06) into `prod_id` and `sales`
* Retains temporal fields: `datum` (date), `Year`, `Month`, `Hour`, `weekday_name`
* Adds audit columns: `_source_file`, `_loaded_at`

**Columns**: `datum`, `Year`, `Month`, `Hour`, `weekday_name`, `prod_id`, `sales`, `_source_file`, `_loaded_at`

**Liquid Clustering**: `datum, prod_id`

#### 2. `bronze_product_dim` (Materialized View)
**Purpose**: Static product and market reference data

**Columns**: `prod_id`, `prod_nm`, `mkt_id`, `mkt_nm`, `_loaded_at`

#### 3. `bronze_time_dim` (Materialized View)
**Purpose**: Time buckets for year-level aggregations (2014-2019)

**Time Buckets**:
* `tb_id` 101-106 → `tb_nm` Y1-Y6
* Each bucket spans Jan 1 - Dec 31 for a calendar year

**Columns**: `tb_id`, `tb_nm`, `strt_dt`, `end_dt`, `_loaded_at`

---

### Silver Layer (`pharma.silver`)

#### 1. `silver_pharma_enriched` (Materialized View)
**Purpose**: Join raw sales with product and time dimensions

**Transformation**:
* Batch read from `bronze_pharma_raw`
* Inner join with `bronze_product_dim` on `prod_id`
* Inner join with `bronze_time_dim` where `datum BETWEEN strt_dt AND end_dt`
* Adds `_processed_at` timestamp

**Columns**: `datum`, `Year`, `Month`, `Hour`, `weekday_name`, `prod_id`, `prod_nm`, `mkt_id`, `mkt_nm`, `tb_id`, `tb_nm`, `sales`, `_source_file`, `_loaded_at`, `_processed_at`

**Note**: Temporal fields (Year, Month, Hour, weekday_name) retained for potential granular drill-down analysis

#### 2. `silver_pharma_aggregated` (Materialized View)
**Purpose**: Aggregate sales to time bucket + product level and calculate market totals

**Transformation**:
* `GROUP BY tb_id, tb_nm, prod_id, prod_nm, mkt_id, mkt_nm`
* `prod_volume = SUM(sales)` for each product
* Window function: `market_volume = SUM(prod_volume) OVER (PARTITION BY tb_id, mkt_id)`
* `market_share_pct = (prod_volume / market_volume) * 100`

**Columns**: `tb_id`, `tb_nm`, `mkt_id`, `mkt_nm`, `prod_id`, `prod_nm`, `prod_volume`, `market_volume`, `market_share_pct`, `_loaded_at`

**Liquid Clustering**: `tb_id, mkt_id, prod_id`

---

### Gold Layer (`pharma.gold`)

#### `gold_pharma_sales` (Materialized View)
**Purpose**: Business-ready analytics with market share ranking and year-over-year growth

**Transformation**:
* Reads from `silver_pharma_aggregated`
* **Product Ranking**: `ROW_NUMBER() OVER (PARTITION BY tb_id, mkt_id ORDER BY market_share_pct DESC)` → ranks products by market share within each time bucket/market
* **YoY Growth**: `LAG(prod_volume) OVER (PARTITION BY prod_id ORDER BY tb_id)` → compares current year volume to previous year
  * Formula: `((current_prod_volume - prev_year_volume) / prev_year_volume) * 100`
* Column renaming for business clarity: `time_bucket_id`, `time_period`, `market_id`, `market_name`, `product_id`, `product_name`, `product_sales_volume`, `total_market_volume`, `market_share_percentage`, `product_rank`, `sales_growth_rate_pct`

**Columns**: `time_bucket_id`, `time_period`, `market_id`, `market_name`, `product_id`, `product_name`, `product_sales_volume`, `total_market_volume`, `market_share_percentage`, `product_rank`, `sales_growth_rate_pct`, `_loaded_at`

**Liquid Clustering**: `time_bucket_id, market_id, product_id`

**Ordering**: `time_bucket_id, market_id, product_rank` (top products first)

---

## Pipeline Configuration

### Current Settings
* **Catalog**: `pharma`
* **Target Schema**: `default` (tables created in bronze/silver/gold schemas)
* **Compute**: Serverless
* **Mode**: Triggered (not continuous)
* **Libraries Path**: `/Repos/prajakta_pandit@berkeley.edu/pharma-sales-pipeline/transformations/**`

### Recommended Schedule
* **Frequency**: Weekly (aligns with batch architecture)
* **Auto Loader**: Checkpoint mechanism ensures only new S3 files are processed on each run
* **Incremental Refresh**: Materialized views automatically detect upstream changes

---

## Running the Pipeline

### Initial Setup
```sql
-- Create schemas (one-time setup)
CREATE SCHEMA IF NOT EXISTS pharma.bronze;
CREATE SCHEMA IF NOT EXISTS pharma.silver;
CREATE SCHEMA IF NOT EXISTS pharma.gold;
```

### Pipeline Execution
1. **Via UI**: Navigate to pipeline page → Click "Start"
2. **Via CLI**: `databricks pipelines start --pipeline-id 396add4f-b43d-4921-b39b-9c64638f585e`
3. **Scheduled**: Configure weekly trigger in pipeline settings

### Monitoring
* **Event Log**: Track file ingestion, table updates, and errors
* **Data Quality**: View row counts and lineage in pipeline monitoring UI
* **Table Versions**: Query historical data via time travel

---

## Querying the Gold Layer

### Example: Top Products by Market Share (2019)
```sql
SELECT 
  time_period,
  market_name,
  product_name,
  product_rank,
  ROUND(market_share_percentage, 2) AS market_share_pct,
  ROUND(sales_growth_rate_pct, 2) AS yoy_growth_pct
FROM pharma.gold.gold_pharma_sales
WHERE time_period = 'Y6'  -- 2019
  AND market_id = 1001
ORDER BY product_rank;
```

### Example: Fastest Growing Products Across All Markets
```sql
SELECT 
  time_period,
  market_name,
  product_name,
  ROUND(sales_growth_rate_pct, 2) AS yoy_growth_pct,
  ROUND(product_sales_volume, 0) AS sales_volume
FROM pharma.gold.gold_pharma_sales
WHERE sales_growth_rate_pct IS NOT NULL
ORDER BY sales_growth_rate_pct DESC
LIMIT 10;
```

---

## Maintenance & Troubleshooting

### Resetting Checkpoints (if reprocessing needed)
```python
# Delete checkpoint to reprocess all files
dbutils.fs.rm("dbfs:/pipelines/<pipeline-id>/checkpoints/bronze_pharma_raw", recurse=True)
```

### Querying Historical Versions
```sql
-- Query table state from 10 versions ago
SELECT * FROM pharma.gold.gold_pharma_sales VERSION AS OF 10;

-- Query table state at specific timestamp
SELECT * FROM pharma.gold.gold_pharma_sales TIMESTAMP AS OF '2026-05-01';
```

### Common Issues
* **Empty Results**: Verify S3 bucket credentials and file paths
* **Duplicate Data**: Check Auto Loader checkpoints are functioning
* **Performance**: Optimize liquid clustering columns based on actual query patterns

---

## Future Enhancements
* Add data quality expectations (e.g., `EXPECT sales >= 0`)
* Implement SCD Type 2 for product dimension changes
* Add quarterly/monthly time buckets for finer granularity
* Create aggregated fact table at market level (star schema)
* Implement change data capture (CDC) for product updates

---

## References
* **Pipeline ID**: `396add4f-b43d-4921-b39b-9c64638f585e`
* **Repository**: `/Repos/prajakta_pandit@berkeley.edu/pharma-sales-pipeline`
* **Databricks Workspace**: Unity Catalog enabled
* **GitHub Integration**: Synced via Git Integration settings
