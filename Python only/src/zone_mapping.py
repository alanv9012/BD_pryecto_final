"""
Zone mapping utilities for assigning pickup and dropoff zones
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, concat
from pyspark.sql.types import IntegerType, StringType
import logging

logger = logging.getLogger(__name__)


def load_zones(spark, zones_path: str) -> DataFrame:
    """
    Load zones dataset from CSV file (custom zones with boundaries)
    
    Args:
        spark: SparkSession instance
        zones_path: Path to zones CSV file
        
    Returns:
        DataFrame with zone definitions
    """
    logger.info(f"Loading zones from {zones_path}")
    
    zones_df = spark.read.option("header", "true").csv(zones_path)
    
    # Ensure proper data types
    zones_df = zones_df.withColumn("zone_id", col("zone_id").cast(IntegerType()))
    zones_df = zones_df.withColumn("min_longitude", col("min_longitude").cast("double"))
    zones_df = zones_df.withColumn("max_longitude", col("max_longitude").cast("double"))
    zones_df = zones_df.withColumn("min_latitude", col("min_latitude").cast("double"))
    zones_df = zones_df.withColumn("max_latitude", col("max_latitude").cast("double"))
    
    logger.info(f"Loaded {zones_df.count()} zones")
    return zones_df


def load_nyc_zone_lookup(spark, lookup_path: str) -> DataFrame:
    """
    Load official NYC taxi zone lookup table
    
    Args:
        spark: SparkSession instance
        lookup_path: Path to taxi_zone_lookup.csv file
        
    Returns:
        DataFrame with LocationID, Borough, Zone, service_zone
    """
    logger.info(f"Loading NYC taxi zone lookup from {lookup_path}")
    
    lookup_df = spark.read.option("header", "true").csv(lookup_path)
    
    # Ensure proper data types
    lookup_df = lookup_df.withColumn("LocationID", col("LocationID").cast(IntegerType()))
    
    # Clean up column names (remove quotes if present)
    lookup_df = lookup_df.select(
        col("LocationID").alias("location_id"),
        col("Borough").alias("borough"),
        col("Zone").alias("zone_name"),
        col("service_zone").alias("service_zone")
    )
    
    logger.info(f"Loaded {lookup_df.count()} zone lookups")
    return lookup_df


def assign_zone(latitude_col: str, longitude_col: str, zones_df: DataFrame) -> DataFrame:
    """
    Assign zone based on latitude and longitude coordinates
    
    This function creates a UDF-like logic using Spark SQL expressions
    to match coordinates to zones.
    
    Args:
        latitude_col: Name of the latitude column
        longitude_col: Name of the longitude column
        zones_df: DataFrame with zone definitions
        
    Returns:
        DataFrame with zone_id column added
    """
    # This is a simplified version. For better performance with large datasets,
    # we'll use a broadcast join approach in the main function
    pass


def enrich_with_zones(trips_df: DataFrame, zones_df: DataFrame) -> DataFrame:
    """
    Enrich trips DataFrame with pickup and dropoff zone information
    
    Args:
        trips_df: Trips DataFrame with latitude/longitude columns or location IDs
        zones_df: Zones DataFrame with zone boundaries (may not be used if using official lookup)
        
    Returns:
        Trips DataFrame with pickup_zone and dropoff_zone columns
    """
    logger.info("Enriching trips with zone information")
    
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import broadcast
    spark = SparkSession.getActiveSession()
    
    # Broadcast zones for efficient join
    zones_broadcast = broadcast(zones_df)
    
    # Check which coordinate columns exist
    has_pickup_coords = 'pickup_latitude' in trips_df.columns and 'pickup_longitude' in trips_df.columns
    has_dropoff_coords = 'dropoff_latitude' in trips_df.columns and 'dropoff_longitude' in trips_df.columns
    
    # Assign pickup zone
    if has_pickup_coords:
        # Create condition for zone matching
        pickup_condition = (
            (col("pickup_latitude") >= col("min_latitude")) &
            (col("pickup_latitude") <= col("max_latitude")) &
            (col("pickup_longitude") >= col("min_longitude")) &
            (col("pickup_longitude") <= col("max_longitude"))
        )
        
        # Join with zones for pickup
        trips_with_pickup_zone = trips_df.crossJoin(zones_broadcast.alias("pickup_zones"))
        trips_with_pickup_zone = trips_with_pickup_zone.filter(
            pickup_condition
        ).select(
            trips_df["*"],
            col("pickup_zones.zone_id").alias("pickup_zone_id"),
            col("pickup_zones.zone_name").alias("pickup_zone_name")
        )
        
        # Keep only the first matching zone (in case of overlaps)
        # Group by trip and take first zone
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number
        
        window_spec = Window.partitionBy([c for c in trips_df.columns]).orderBy("pickup_zone_id")
        trips_with_pickup_zone = trips_with_pickup_zone.withColumn(
            "row_num", row_number().over(window_spec)
        ).filter(col("row_num") == 1).drop("row_num")
        
        trips_df = trips_with_pickup_zone
    else:
        # If no coordinates, use location IDs with official NYC zone lookup table
        if 'pickup_location_id' in trips_df.columns:
            # Try to load official NYC zone lookup table
            from src.config import ZONES_LOOKUP_PATH
            try:
                if ZONES_LOOKUP_PATH.exists():
                    zone_lookup = load_nyc_zone_lookup(spark, str(ZONES_LOOKUP_PATH))
                    from pyspark.sql.functions import broadcast
                    zone_lookup_broadcast = broadcast(zone_lookup)
                    
                    # Join with zone lookup to get official zone names
                    trips_df = trips_df.join(
                        zone_lookup_broadcast.alias("pickup_lookup"),
                        trips_df["pickup_location_id"] == col("pickup_lookup.location_id"),
                        "left"
                    ).withColumn(
                        "pickup_zone_id",
                        col("pickup_location_id")
                    ).withColumn(
                        "pickup_zone_name",
                        col("pickup_lookup.zone_name")
                    ).withColumn(
                        "pickup_borough",
                        col("pickup_lookup.borough")
                    ).withColumn(
                        "pickup_service_zone",
                        col("pickup_lookup.service_zone")
                    ).drop("pickup_lookup.location_id", "pickup_lookup.zone_name", "pickup_lookup.borough", "pickup_lookup.service_zone")
                    
                    logger.info("Using official NYC zone lookup table for pickup zones")
                else:
                    # Fallback: use location IDs directly
                    trips_df = trips_df.withColumn(
                        "pickup_zone_id",
                        col("pickup_location_id")
                    ).withColumn(
                        "pickup_zone_name",
                        concat(lit("Zone_"), col("pickup_location_id").cast("string"))
                    )
                    logger.info("Using pickup_location_id directly as pickup_zone_id (no lookup table found)")
            except Exception as e:
                logger.warning(f"Error loading zone lookup table: {e}. Using location IDs directly.")
                trips_df = trips_df.withColumn(
                    "pickup_zone_id",
                    col("pickup_location_id")
                ).withColumn(
                    "pickup_zone_name",
                    concat(lit("Zone_"), col("pickup_location_id").cast("string"))
                )
        else:
            trips_df = trips_df.withColumn("pickup_zone_id", lit(None).cast(IntegerType()))
            trips_df = trips_df.withColumn("pickup_zone_name", lit(None).cast(StringType()))
            logger.warning("No pickup_location_id found - pickup zones will be NULL")
    
    # Assign dropoff zone
    if has_dropoff_coords:
        dropoff_condition = (
            (col("dropoff_latitude") >= col("min_latitude")) &
            (col("dropoff_latitude") <= col("max_latitude")) &
            (col("dropoff_longitude") >= col("min_longitude")) &
            (col("dropoff_longitude") <= col("max_longitude"))
        )
        
        zones_broadcast_dropoff = broadcast(zones_df)
        trips_with_dropoff_zone = trips_df.crossJoin(zones_broadcast_dropoff.alias("dropoff_zones"))
        trips_with_dropoff_zone = trips_with_dropoff_zone.filter(
            dropoff_condition
        ).select(
            trips_df["*"],
            col("dropoff_zones.zone_id").alias("dropoff_zone_id"),
            col("dropoff_zones.zone_name").alias("dropoff_zone_name")
        )
        
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number
        
        window_spec = Window.partitionBy([c for c in trips_df.columns]).orderBy("dropoff_zone_id")
        trips_with_dropoff_zone = trips_with_dropoff_zone.withColumn(
            "row_num", row_number().over(window_spec)
        ).filter(col("row_num") == 1).drop("row_num")
        
        trips_df = trips_with_dropoff_zone
    else:
        # Use location IDs with official NYC zone lookup table for dropoff
        if 'dropoff_location_id' in trips_df.columns:
            # Try to load official NYC zone lookup table
            from src.config import ZONES_LOOKUP_PATH
            try:
                if ZONES_LOOKUP_PATH.exists():
                    zone_lookup = load_nyc_zone_lookup(spark, str(ZONES_LOOKUP_PATH))
                    from pyspark.sql.functions import broadcast
                    zone_lookup_broadcast = broadcast(zone_lookup)
                    
                    # Join with zone lookup to get official zone names
                    trips_df = trips_df.join(
                        zone_lookup_broadcast.alias("dropoff_lookup"),
                        trips_df["dropoff_location_id"] == col("dropoff_lookup.location_id"),
                        "left"
                    ).withColumn(
                        "dropoff_zone_id",
                        col("dropoff_location_id")
                    ).withColumn(
                        "dropoff_zone_name",
                        col("dropoff_lookup.zone_name")
                    ).withColumn(
                        "dropoff_borough",
                        col("dropoff_lookup.borough")
                    ).withColumn(
                        "dropoff_service_zone",
                        col("dropoff_lookup.service_zone")
                    ).drop("dropoff_lookup.location_id", "dropoff_lookup.zone_name", "dropoff_lookup.borough", "dropoff_lookup.service_zone")
                    
                    logger.info("Using official NYC zone lookup table for dropoff zones")
                else:
                    # Fallback: use location IDs directly
                    trips_df = trips_df.withColumn(
                        "dropoff_zone_id",
                        col("dropoff_location_id")
                    ).withColumn(
                        "dropoff_zone_name",
                        concat(lit("Zone_"), col("dropoff_location_id").cast("string"))
                    )
                    logger.info("Using dropoff_location_id directly as dropoff_zone_id (no lookup table found)")
            except Exception as e:
                logger.warning(f"Error loading zone lookup table: {e}. Using location IDs directly.")
                trips_df = trips_df.withColumn(
                    "dropoff_zone_id",
                    col("dropoff_location_id")
                ).withColumn(
                    "dropoff_zone_name",
                    concat(lit("Zone_"), col("dropoff_location_id").cast("string"))
                )
        else:
            trips_df = trips_df.withColumn("dropoff_zone_id", lit(None).cast(IntegerType()))
            trips_df = trips_df.withColumn("dropoff_zone_name", lit(None).cast(StringType()))
            logger.warning("No dropoff_location_id found - dropoff zones will be NULL")
    
    logger.info("Zone enrichment complete")
    return trips_df


def create_zones_from_data(trips_df: DataFrame, num_zones: int = 20) -> DataFrame:
    """
    Create zones by dividing the coordinate space into a grid
    This is a fallback if no zones dataset is provided
    
    Args:
        trips_df: Trips DataFrame with coordinates
        num_zones: Number of zones to create (will create a grid)
        
    Returns:
        DataFrame with zone definitions
    """
    logger.info(f"Creating {num_zones} zones from trip data")
    
    from pyspark.sql.functions import min as spark_min, max as spark_max
    from src.config import NYC_BOUNDS
    
    # Get coordinate bounds from data or use defaults
    if 'pickup_latitude' in trips_df.columns:
        bounds = trips_df.agg(
            spark_min("pickup_latitude").alias("min_lat"),
            spark_max("pickup_latitude").alias("max_lat"),
            spark_min("pickup_longitude").alias("min_lon"),
            spark_max("pickup_longitude").alias("max_lon")
        ).collect()[0]
        
        min_lat = bounds["min_lat"] or NYC_BOUNDS["min_latitude"]
        max_lat = bounds["max_lat"] or NYC_BOUNDS["max_latitude"]
        min_lon = bounds["min_lon"] or NYC_BOUNDS["min_longitude"]
        max_lon = bounds["max_lon"] or NYC_BOUNDS["max_longitude"]
    else:
        min_lat = NYC_BOUNDS["min_latitude"]
        max_lat = NYC_BOUNDS["max_latitude"]
        min_lon = NYC_BOUNDS["min_longitude"]
        max_lon = NYC_BOUNDS["max_longitude"]
    
    # Create grid zones
    import math
    grid_size = int(math.sqrt(num_zones))
    
    zones_data = []
    zone_id = 1
    lat_step = (max_lat - min_lat) / grid_size
    lon_step = (max_lon - min_lon) / grid_size
    
    for i in range(grid_size):
        for j in range(grid_size):
            zones_data.append({
                "zone_id": zone_id,
                "zone_name": f"Zone_{zone_id}",
                "min_latitude": min_lat + i * lat_step,
                "max_latitude": min_lat + (i + 1) * lat_step,
                "min_longitude": min_lon + j * lon_step,
                "max_longitude": min_lon + (j + 1) * lon_step
            })
            zone_id += 1
    
    # Create DataFrame from zones data
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    zones_df = spark.createDataFrame(zones_data)
    
    logger.info(f"Created {zones_df.count()} zones")
    return zones_df


