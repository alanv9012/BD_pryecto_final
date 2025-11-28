"""
Analysis functions for generating aggregated statistics
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    count, sum as spark_sum, avg, min as spark_min, max as spark_max,
    col, desc, asc, when, countDistinct, lit, concat
)
import logging

logger = logging.getLogger(__name__)


def analyze_demand_by_hour(df: DataFrame) -> DataFrame:
    """
    Analyze trip demand by hour of day
    
    Args:
        df: Preprocessed trips DataFrame
        
    Returns:
        DataFrame with hourly demand statistics
    """
    logger.info("Analyzing demand by hour of day")
    
    if 'hour_of_day' not in df.columns:
        logger.warning("hour_of_day column not found, skipping analysis")
        return None
    
    hourly_stats = df.groupBy("hour_of_day").agg(
        count("*").alias("total_trips"),
        avg("trip_duration_min").alias("avg_duration_min"),
        avg("trip_distance_km").alias("avg_distance_km"),
        spark_sum("fare_amount").alias("total_revenue"),
        avg("fare_amount").alias("avg_fare_amount")
    ).orderBy("hour_of_day")
    
    return hourly_stats


def analyze_demand_by_day(df: DataFrame) -> DataFrame:
    """
    Analyze trip demand by day of week
    
    Args:
        df: Preprocessed trips DataFrame
        
    Returns:
        DataFrame with daily demand statistics
    """
    logger.info("Analyzing demand by day of week")
    
    if 'day_of_week' not in df.columns:
        logger.warning("day_of_week column not found, skipping analysis")
        return None
    
    daily_stats = df.groupBy("day_of_week", "day_name").agg(
        count("*").alias("total_trips"),
        avg("trip_duration_min").alias("avg_duration_min"),
        avg("trip_distance_km").alias("avg_distance_km"),
        spark_sum("fare_amount").alias("total_revenue"),
        avg("fare_amount").alias("avg_fare_amount")
    ).orderBy("day_of_week")
    
    # Add weekday/weekend flag
    daily_stats = daily_stats.withColumn(
        "is_weekend",
        when(col("day_of_week").isin([1, 7]), "Weekend").otherwise("Weekday")
    )
    
    return daily_stats


def analyze_zone_activity(df: DataFrame) -> DataFrame:
    """
    Analyze zone activity (top zones by origin and destination)
    
    Args:
        df: Preprocessed trips DataFrame with zone information
        
    Returns:
        Dictionary with origin and destination zone statistics
    """
    logger.info("Analyzing zone activity")
    
    results = {}
    
    # Top zones by origin
    if 'pickup_zone_name' in df.columns:
        origin_stats = df.groupBy("pickup_zone_id", "pickup_zone_name").agg(
            count("*").alias("total_trips_origin"),
            spark_sum("fare_amount").alias("total_revenue_origin"),
            avg("fare_amount").alias("avg_fare_origin")
        ).orderBy(desc("total_trips_origin"))
        
        results["top_origin_zones"] = origin_stats.limit(10)
    else:
        logger.warning("pickup_zone_name not found, skipping origin zone analysis")
        results["top_origin_zones"] = None
    
    # Top zones by destination
    if 'dropoff_zone_name' in df.columns:
        dest_stats = df.groupBy("dropoff_zone_id", "dropoff_zone_name").agg(
            count("*").alias("total_trips_destination"),
            spark_sum("fare_amount").alias("total_revenue_destination"),
            avg("fare_amount").alias("avg_fare_destination")
        ).orderBy(desc("total_trips_destination"))
        
        results["top_destination_zones"] = dest_stats.limit(10)
    else:
        logger.warning("dropoff_zone_name not found, skipping destination zone analysis")
        results["top_destination_zones"] = None
    
    # Combined zone activity (both origin and destination)
    if 'pickup_zone_name' in df.columns and 'dropoff_zone_name' in df.columns:
        # Create a union of all zones (both pickup and dropoff)
        pickup_zones = df.select(
            col("pickup_zone_id").alias("zone_id"),
            col("pickup_zone_name").alias("zone_name"),
            col("fare_amount")
        )
        
        dropoff_zones = df.select(
            col("dropoff_zone_id").alias("zone_id"),
            col("dropoff_zone_name").alias("zone_name"),
            col("fare_amount")
        )
        
        all_zones = pickup_zones.union(dropoff_zones)
        
        combined_stats = all_zones.groupBy("zone_id", "zone_name").agg(
            count("*").alias("total_activity"),
            spark_sum("fare_amount").alias("total_revenue"),
            avg("fare_amount").alias("avg_fare")
        ).orderBy(desc("total_activity"))
        
        results["combined_zone_activity"] = combined_stats.limit(20)
    else:
        results["combined_zone_activity"] = None
    
    return results


def analyze_trip_duration_distance(df: DataFrame) -> DataFrame:
    """
    Analyze trip duration and distance patterns
    
    Args:
        df: Preprocessed trips DataFrame
        
    Returns:
        DataFrame with duration and distance statistics
    """
    logger.info("Analyzing trip duration and distance patterns")
    
    stats = []
    
    # By hour
    if 'hour_of_day' in df.columns:
        hourly_duration = df.groupBy("hour_of_day").agg(
            avg("trip_duration_min").alias("avg_duration_min"),
            spark_min("trip_duration_min").alias("min_duration_min"),
            spark_max("trip_duration_min").alias("max_duration_min"),
            avg("trip_distance_km").alias("avg_distance_km"),
            spark_min("trip_distance_km").alias("min_distance_km"),
            spark_max("trip_distance_km").alias("max_distance_km")
        ).orderBy("hour_of_day")
        stats.append(("by_hour", hourly_duration))
    
    # By zone
    if 'pickup_zone_name' in df.columns:
        zone_duration = df.groupBy("pickup_zone_name").agg(
            avg("trip_duration_min").alias("avg_duration_min"),
            avg("trip_distance_km").alias("avg_distance_km"),
            count("*").alias("trip_count")
        ).orderBy(desc("trip_count"))
        stats.append(("by_zone", zone_duration))
    
    return stats


def analyze_revenue_payment(df: DataFrame) -> DataFrame:
    """
    Analyze revenue by payment type
    
    Args:
        df: Preprocessed trips DataFrame
        
    Returns:
        DataFrame with revenue statistics by payment type
    """
    logger.info("Analyzing revenue by payment type")
    
    if 'payment_type' not in df.columns:
        logger.warning("payment_type column not found, skipping analysis")
        return None
    
    # First, let's see what payment_type values actually exist in the data
    payment_type_dist = df.groupBy("payment_type").agg(
        count("*").alias("count")
    ).orderBy(desc("count")).collect()
    
    logger.info("Payment type distribution in data:")
    for row in payment_type_dist[:10]:  # Log top 10
        logger.info(f"  Payment Type {row['payment_type']}: {row['count']:,} trips")
    
    # Map payment type codes to names (NYC taxi standard codes)
    # Standard codes: 1=Credit Card, 2=Cash, 3=No Charge, 4=Dispute, 5=Unknown, 6=Voided Trip
    # Some datasets use: 0=Unknown/Not Specified, NULL=Unknown
    # Based on your data: 0 appears frequently (likely "Unknown" or "Not Specified")
    df_with_payment_name = df.withColumn(
        "payment_type_name",
        when(col("payment_type").isNull(), lit("Unknown/Not Specified"))
        .when(col("payment_type") == 0, lit("Unknown/Not Specified"))
        .when(col("payment_type") == 1, lit("Credit Card"))
        .when(col("payment_type") == 2, lit("Cash"))
        .when(col("payment_type") == 3, lit("No Charge"))
        .when(col("payment_type") == 4, lit("Dispute"))
        .when(col("payment_type") == 5, lit("Unknown"))
        .when(col("payment_type") == 6, lit("Voided Trip"))
        .otherwise(concat(lit("Other ("), col("payment_type").cast("string"), lit(")")))
    )
    
    revenue_stats = df_with_payment_name.groupBy("payment_type", "payment_type_name").agg(
        count("*").alias("total_trips"),
        spark_sum("fare_amount").alias("total_revenue"),
        avg("fare_amount").alias("avg_revenue_per_trip")
    ).orderBy(desc("total_revenue"))
    
    return revenue_stats


