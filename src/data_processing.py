"""
Data processing utilities for taxi data cleaning and transformation
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, isnan, isnull, to_timestamp, hour, dayofweek,
    datediff, minute, second, lit, round, abs as spark_abs
)
from pyspark.sql.types import DoubleType, IntegerType, TimestampType, NumericType
import logging

logger = logging.getLogger(__name__)


def create_spark_session(config: dict = None) -> SparkSession:
    """
    Create and configure a Spark session
    
    Args:
        config: Dictionary with Spark configuration options
        
    Returns:
        Configured SparkSession
    """
    if config is None:
        from src.config import SPARK_CONFIG
        config = SPARK_CONFIG
    
    builder = SparkSession.builder
    
    for key, value in config.items():
        builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    logger.info("Spark session created successfully")
    return spark


def load_taxi_data(spark: SparkSession, data_dir: str) -> DataFrame:
    """
    Load all parquet files from the taxi dataset directory
    
    Args:
        spark: SparkSession instance
        data_dir: Path to directory containing parquet files
        
    Returns:
        Combined DataFrame with all taxi trips
    """
    from pathlib import Path
    
    logger.info(f"Loading taxi data from {data_dir}")
    
    # Convert to Path object to handle spaces and normalize
    data_path = Path(data_dir)
    
    # Check if directory exists
    if not data_path.exists():
        raise FileNotFoundError(f"Taxi dataset directory not found: {data_dir}")
    
    # Get all parquet files
    parquet_files = list(data_path.glob("*.parquet"))
    
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in: {data_dir}")
    
    logger.info(f"Found {len(parquet_files)} parquet files")
    
    # Convert paths to strings and handle spaces properly
    # Use absolute paths to avoid issues with spaces
    parquet_paths = [str(f.absolute()) for f in parquet_files]
    
    # Load all parquet files by passing list of paths
    # This works with paths containing spaces, unlike glob pattern
    df = spark.read.parquet(*parquet_paths)
    
    logger.info(f"Loaded {df.count()} total records")
    return df


def clean_taxi_data(df: DataFrame) -> DataFrame:
    """
    Clean the taxi data by removing nulls and invalid records
    
    Args:
        df: Raw taxi DataFrame
        
    Returns:
        Cleaned DataFrame
    """
    logger.info("Starting data cleaning process")
    
    initial_count = df.count()
    
    # Remove records with nulls in key fields
    # Common column names in NYC taxi data
    key_fields = []
    
    # Check which columns exist and add them to key_fields
    available_columns = df.columns
    
    # Common variations of column names
    pickup_time_cols = [c for c in available_columns if 'pickup' in c.lower() and 'time' in c.lower()]
    dropoff_time_cols = [c for c in available_columns if 'dropoff' in c.lower() and 'time' in c.lower()]
    distance_cols = [c for c in available_columns if 'distance' in c.lower()]
    fare_cols = [c for c in available_columns if 'fare' in c.lower() and 'amount' in c.lower()]
    
    if pickup_time_cols:
        key_fields.append(pickup_time_cols[0])
    if dropoff_time_cols:
        key_fields.append(dropoff_time_cols[0])
    if distance_cols:
        key_fields.append(distance_cols[0])
    if fare_cols:
        key_fields.append(fare_cols[0])
    
    # Get column types to check which ones are numeric
    column_types = {field.name: field.dataType for field in df.schema.fields}
    
    # Remove nulls and NaN values in key fields
    for field in key_fields:
        before_count = df.count()
        df = df.filter(col(field).isNotNull())
        after_count = df.count()
        logger.info(f"After filtering nulls in {field}: {before_count} -> {after_count} records")
        
        # Only use isnan() for numeric types (timestamps and strings don't have NaN)
        col_type = column_types.get(field)
        if isinstance(col_type, NumericType):
            before_count = df.count()
            df = df.filter(~isnan(col(field)))
            after_count = df.count()
            logger.info(f"After filtering NaN in {field}: {before_count} -> {after_count} records")
    
    # Remove duplicates based on key fields (pickup time, dropoff time, distance, fare)
    # Only remove duplicates if we have enough key fields to identify unique trips
    if len(key_fields) >= 2:
        before_count = df.count()
        df = df.dropDuplicates(key_fields[:2])  # Use first 2 key fields (usually pickup and dropoff datetime)
        after_count = df.count()
        logger.info(f"After removing duplicates: {before_count} -> {after_count} records")
    elif 'trip_id' in available_columns:
        # Only use trip_id if it exists and is actually a unique identifier
        before_count = df.count()
        df = df.dropDuplicates(['trip_id'])
        after_count = df.count()
        logger.info(f"After removing duplicates by trip_id: {before_count} -> {after_count} records")
    
    cleaned_count = df.count()
    logger.info(f"Cleaned data: {initial_count} -> {cleaned_count} records ({initial_count - cleaned_count} removed)")
    
    return df


def standardize_column_names(df: DataFrame) -> DataFrame:
    """
    Standardize column names to match expected schema
    
    Args:
        df: DataFrame with potentially non-standard column names
        
    Returns:
        DataFrame with standardized column names
    """
    logger.info("Standardizing column names")
    
    column_mapping = {}
    available_columns = df.columns
    
    # Map common NYC taxi column variations to standard names
    mappings = {
        'tpep_pickup_datetime': 'pickup_datetime',
        'tpep_dropoff_datetime': 'dropoff_datetime',
        'pickup_datetime': 'pickup_datetime',
        'dropoff_datetime': 'dropoff_datetime',
        'trip_distance': 'trip_distance_km',
        'trip_distance_km': 'trip_distance_km',
        'fare_amount': 'fare_amount',
        'total_amount': 'fare_amount',  # Fallback if fare_amount doesn't exist
        'passenger_count': 'passenger_count',
        'payment_type': 'payment_type',
        'PULocationID': 'pickup_location_id',
        'DOLocationID': 'dropoff_location_id',
        'pickup_longitude': 'pickup_longitude',
        'pickup_latitude': 'pickup_latitude',
        'dropoff_longitude': 'dropoff_longitude',
        'dropoff_latitude': 'dropoff_latitude'
    }
    
    for old_name, new_name in mappings.items():
        if old_name in available_columns and new_name not in available_columns:
            column_mapping[old_name] = new_name
    
    # Rename columns
    for old_name, new_name in column_mapping.items():
        df = df.withColumnRenamed(old_name, new_name)
    
    return df


def add_derived_columns(df: DataFrame) -> DataFrame:
    """
    Add derived columns: trip_duration_min, hour_of_day, day_of_week
    
    Args:
        df: Cleaned DataFrame
        
    Returns:
        DataFrame with derived columns
    """
    logger.info("Adding derived columns")
    
    from src.config import MIN_TRIP_DURATION_MIN, MAX_TRIP_DURATION_MIN
    
    # Convert datetime columns if they're strings
    if 'pickup_datetime' in df.columns:
        # Try to convert to timestamp if it's a string
        df = df.withColumn(
            "pickup_datetime",
            when(col("pickup_datetime").cast("string").isNotNull(),
                 to_timestamp(col("pickup_datetime"), "yyyy-MM-dd HH:mm:ss"))
            .otherwise(col("pickup_datetime"))
        )
    
    if 'dropoff_datetime' in df.columns:
        df = df.withColumn(
            "dropoff_datetime",
            when(col("dropoff_datetime").cast("string").isNotNull(),
                 to_timestamp(col("dropoff_datetime"), "yyyy-MM-dd HH:mm:ss"))
            .otherwise(col("dropoff_datetime"))
        )
    
    # Calculate trip duration in minutes
    if 'pickup_datetime' in df.columns and 'dropoff_datetime' in df.columns:
        df = df.withColumn(
            "trip_duration_min",
            round(
                (col("dropoff_datetime").cast("long") - col("pickup_datetime").cast("long")) / 60.0,
                2
            )
        )
    else:
        df = df.withColumn("trip_duration_min", lit(None).cast(DoubleType()))
    
    # Extract hour of day
    if 'pickup_datetime' in df.columns:
        df = df.withColumn("hour_of_day", hour(col("pickup_datetime")))
    else:
        df = df.withColumn("hour_of_day", lit(None).cast(IntegerType()))
    
    # Extract day of week (1=Sunday, 2=Monday, ..., 7=Saturday)
    if 'pickup_datetime' in df.columns:
        df = df.withColumn("day_of_week", dayofweek(col("pickup_datetime")))
    else:
        df = df.withColumn("day_of_week", lit(None).cast(IntegerType()))
    
    # Add day name for readability
    from pyspark.sql.functions import coalesce
    day_names = {
        1: "Sunday", 2: "Monday", 3: "Tuesday", 4: "Wednesday",
        5: "Thursday", 6: "Friday", 7: "Saturday"
    }
    
    # Create day_name column using nested when statements
    day_name_expr = when(col("day_of_week") == 1, "Sunday")
    for day_num in range(2, 8):
        day_name_expr = day_name_expr.when(col("day_of_week") == day_num, day_names[day_num])
    day_name_expr = day_name_expr.otherwise("Unknown")
    
    df = df.withColumn("day_name", day_name_expr)
    
    # Filter out invalid trip durations
    df = df.filter(
        (col("trip_duration_min") >= MIN_TRIP_DURATION_MIN) &
        (col("trip_duration_min") <= MAX_TRIP_DURATION_MIN)
    )
    
    return df


def filter_outliers(df: DataFrame) -> DataFrame:
    """
    Filter out outliers based on business rules
    
    Args:
        df: DataFrame with derived columns
        
    Returns:
        DataFrame with outliers removed
    """
    logger.info("Filtering outliers")
    
    from src.config import (
        MIN_TRIP_DISTANCE_KM, MAX_TRIP_DISTANCE_KM,
        MIN_FARE_AMOUNT, MAX_FARE_AMOUNT
    )
    
    initial_count = df.count()
    
    # Filter distance outliers
    if 'trip_distance_km' in df.columns:
        df = df.filter(
            (col("trip_distance_km") >= MIN_TRIP_DISTANCE_KM) &
            (col("trip_distance_km") <= MAX_TRIP_DISTANCE_KM)
        )
    
    # Filter fare outliers
    if 'fare_amount' in df.columns:
        df = df.filter(
            (col("fare_amount") >= MIN_FARE_AMOUNT) &
            (col("fare_amount") <= MAX_FARE_AMOUNT)
        )
    
    # Filter trips with distance = 0 but high duration
    if 'trip_distance_km' in df.columns and 'trip_duration_min' in df.columns:
        df = df.filter(
            ~((col("trip_distance_km") == 0) & (col("trip_duration_min") > 30))
        )
    
    filtered_count = df.count()
    logger.info(f"Outlier filtering: {initial_count} -> {filtered_count} records ({initial_count - filtered_count} removed)")
    
    return df


def preprocess_taxi_data(spark: SparkSession, data_dir: str) -> DataFrame:
    """
    Complete preprocessing pipeline: load, clean, transform
    
    Args:
        spark: SparkSession instance
        data_dir: Path to taxi data directory
        
    Returns:
        Fully preprocessed DataFrame
    """
    logger.info("Starting complete preprocessing pipeline")
    
    # Load data
    df = load_taxi_data(spark, data_dir)
    
    # Standardize column names
    df = standardize_column_names(df)
    
    # Clean data
    df = clean_taxi_data(df)
    
    # Add derived columns
    df = add_derived_columns(df)
    
    # Filter outliers
    df = filter_outliers(df)
    
    logger.info(f"Preprocessing complete. Final record count: {df.count()}")
    
    return df

