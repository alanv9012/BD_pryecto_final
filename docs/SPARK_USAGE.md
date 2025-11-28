# How This Project Uses Apache Spark

This document explains in detail how Apache Spark (PySpark) is utilized throughout the Urban Mobility Pattern Analysis project.

## Overview

Apache Spark serves as the **core distributed processing engine** for handling large-scale taxi trip data. The project processes 10 months of NYC taxi data (potentially millions of records) efficiently using Spark's distributed computing capabilities.

## 1. Spark Session Management

### Session Creation
```python
# From src/data_processing.py
spark = SparkSession.builder
    .appName("UrbanMobilityAnalysis")
    .master("local[*]")  # Uses all CPU cores
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
```

**Key Features:**
- **Local Mode (`local[*]`)**: Uses all available CPU cores on your machine
- **Adaptive Query Execution**: Automatically optimizes query execution
- **Memory Configuration**: 4GB allocated for driver and executor

## 2. Data Loading with Spark

### Reading Parquet Files
```python
# Loads ALL parquet files from directory in parallel
df = spark.read.parquet("Taxi Dataset/*.parquet")
```

**Spark Benefits:**
- **Distributed Reading**: Spark automatically partitions and reads multiple files in parallel
- **Schema Inference**: Automatically detects schema from parquet metadata
- **Lazy Evaluation**: Only executes when an action is called (like `.count()`)

**Why Spark?**
- Handles large datasets that don't fit in memory
- Processes 10 monthly files simultaneously
- Leverages multiple CPU cores automatically

## 3. Data Cleaning Operations

### Spark SQL Functions Used

#### a) Null Value Handling
```python
from pyspark.sql.functions import col, isnull, isnan, when, count

# Count nulls across all columns
null_counts = df.select([
    count(when(isnull(c) | isnan(c), c)).alias(c) 
    for c in df.columns
])

# Filter out nulls
df = df.filter(col("pickup_datetime").isNotNull())
```

#### b) Data Type Conversions
```python
from pyspark.sql.functions import to_timestamp

# Convert string dates to timestamps
df = df.withColumn(
    "pickup_datetime",
    to_timestamp(col("pickup_datetime"), "yyyy-MM-dd HH:mm:ss")
)
```

**Spark Benefits:**
- **Vectorized Operations**: Processes columns efficiently
- **Distributed Filtering**: Filters data across partitions in parallel
- **Type Safety**: Ensures data types are correct before processing

## 4. Data Transformations

### Derived Columns Creation

#### a) Trip Duration Calculation
```python
from pyspark.sql.functions import col, round

df = df.withColumn(
    "trip_duration_min",
    round(
        (col("dropoff_datetime").cast("long") - 
         col("pickup_datetime").cast("long")) / 60.0,
        2
    )
)
```

#### b) Temporal Features Extraction
```python
from pyspark.sql.functions import hour, dayofweek, when

# Extract hour of day (0-23)
df = df.withColumn("hour_of_day", hour(col("pickup_datetime")))

# Extract day of week (1=Sunday, 7=Saturday)
df = df.withColumn("day_of_week", dayofweek(col("pickup_datetime")))

# Map to day names
df = df.withColumn(
    "day_name",
    when(col("day_of_week") == 1, "Sunday")
    .when(col("day_of_week") == 2, "Monday")
    # ... etc
)
```

**Spark Benefits:**
- **Expression Optimization**: Spark optimizes these transformations
- **Columnar Processing**: Efficient column-based operations
- **Lazy Evaluation**: Builds transformation plan before executing

## 5. Aggregations and Analysis

### GroupBy Operations

#### a) Hourly Demand Analysis
```python
from pyspark.sql.functions import count, avg, sum as spark_sum

hourly_stats = df.groupBy("hour_of_day").agg(
    count("*").alias("total_trips"),
    avg("trip_duration_min").alias("avg_duration_min"),
    avg("trip_distance_km").alias("avg_distance_km"),
    spark_sum("fare_amount").alias("total_revenue"),
    avg("fare_amount").alias("avg_fare_amount")
)
```

#### b) Zone Activity Analysis
```python
zone_stats = df.groupBy("pickup_zone_name").agg(
    count("*").alias("total_trips_origin"),
    spark_sum("fare_amount").alias("total_revenue_origin"),
    avg("fare_amount").alias("avg_fare_origin")
).orderBy(desc("total_trips_origin"))
```

**Spark Benefits:**
- **Distributed Aggregations**: Computes aggregations across partitions in parallel
- **Optimized Shuffles**: Efficient data movement for groupBy operations
- **Memory Management**: Handles large result sets efficiently

## 6. Joins and Data Enrichment

### Zone Assignment with Joins
```python
from pyspark.sql.functions import broadcast

# Broadcast small zones table to all nodes
zones_broadcast = broadcast(zones_df)

# Join with zone boundaries
trips_with_zones = trips_df.crossJoin(zones_broadcast).filter(
    (col("pickup_latitude") >= col("min_latitude")) &
    (col("pickup_latitude") <= col("max_latitude")) &
    # ... longitude conditions
)
```

**Spark Benefits:**
- **Broadcast Joins**: Efficiently shares small lookup tables
- **Partition-Aware Joins**: Optimizes join strategies automatically
- **Handles Large Datasets**: Processes millions of trips efficiently

## 7. Window Functions

### Ranking Zones
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy(...).orderBy("pickup_zone_id")
df = df.withColumn("row_num", row_number().over(window_spec))
```

**Spark Benefits:**
- **Distributed Window Operations**: Computes windows in parallel
- **Efficient Partitioning**: Minimizes data shuffling

## 8. Data Export

### Converting to Pandas (for MongoDB/CSV export)
```python
# For smaller aggregated results
pandas_df = spark_df.toPandas()
pandas_df.to_csv("output/hourly_demand.csv")
```

**When to Use:**
- Aggregated results are small enough for pandas
- For exports to MongoDB or CSV files
- Spark handles the conversion efficiently

## 9. Spark Features Leveraged

### a) Lazy Evaluation
- Spark builds a logical plan first
- Only executes when an action (like `.count()`, `.show()`, `.collect()`) is called
- Enables query optimization

### b) Catalyst Optimizer
- Automatically optimizes query plans
- Predicate pushdown, column pruning, constant folding
- Adaptive Query Execution (AQE) adjusts at runtime

### c) Distributed Processing
- Automatically partitions data across cores
- Processes partitions in parallel
- Handles data that exceeds RAM size

### d) Fault Tolerance
- Tracks lineage of transformations
- Can recompute lost partitions automatically
- Resilient Distributed Datasets (RDDs) under the hood

## 10. Performance Optimizations Used

### a) Adaptive Query Execution (AQE)
```python
"spark.sql.adaptive.enabled": "true"
"spark.sql.adaptive.coalescePartitions.enabled": "true"
```
- Dynamically adjusts query execution
- Coalesces small partitions for efficiency

### b) Broadcast Joins
- Used for zone lookups (small table)
- Reduces network traffic
- Improves join performance

### c) Column Pruning
- Only reads columns needed for analysis
- Reduces I/O and memory usage

## 11. Memory Management

### Configuration
```python
"spark.driver.memory": "4g"      # Driver process memory
"spark.executor.memory": "4g"    # Executor process memory
```

**Why This Matters:**
- Driver: Coordinates jobs and manages metadata
- Executor: Processes data partitions
- Proper sizing prevents out-of-memory errors

## 12. Real-World Example Workflow

```python
# 1. Load (Lazy - doesn't execute yet)
df = spark.read.parquet("Taxi Dataset/*.parquet")

# 2. Transform (Lazy - builds plan)
df_cleaned = df.filter(col("fare_amount").isNotNull())
df_with_hour = df_cleaned.withColumn("hour", hour(col("pickup_datetime")))

# 3. Aggregate (Lazy - builds plan)
hourly_stats = df_with_hour.groupBy("hour").agg(
    count("*").alias("trips")
)

# 4. Action (EXECUTES - triggers computation)
results = hourly_stats.collect()  # or .show(), .toPandas(), etc.
```

## 13. Advantages Over Pandas/Regular Python

| Aspect | Pandas | Spark |
|--------|--------|-------|
| **Data Size** | Limited by RAM | Can exceed RAM (uses disk) |
| **Parallelism** | Single core | Multiple cores automatically |
| **Processing** | Row-by-row | Columnar & vectorized |
| **Scalability** | Single machine | Can scale to clusters |
| **Performance** | Fast for small data | Fast for large data |

## 14. When Spark Shines in This Project

1. **Loading Multiple Files**: Processes 10 parquet files simultaneously
2. **Large-Scale Filtering**: Filters millions of records efficiently
3. **Complex Aggregations**: Groups by hour, day, zone across all data
4. **Join Operations**: Enriches trips with zone information
5. **Memory Efficiency**: Handles datasets larger than available RAM

## 15. Summary

Apache Spark is the **backbone** of this project, enabling:

✅ **Distributed Processing**: Uses all CPU cores automatically  
✅ **Large Data Handling**: Processes datasets that exceed RAM  
✅ **Efficient Operations**: Optimized transformations and aggregations  
✅ **Scalability**: Can easily move to a cluster if needed  
✅ **Performance**: Fast processing of millions of records  

Without Spark, processing 10 months of taxi data would be:
- Much slower (single-threaded)
- Limited by RAM size
- Require manual chunking and merging
- More complex error handling

Spark makes big data processing accessible and efficient!

