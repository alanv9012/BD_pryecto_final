"""
Configuration settings for the project
"""

import os
from pathlib import Path

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent

# Data paths
# Taxi dataset can be in the current directory or parent directory
TAXI_DATA_DIR = PROJECT_ROOT / "Taxi Dataset"
if not TAXI_DATA_DIR.exists():
    TAXI_DATA_DIR = PROJECT_ROOT.parent / "Taxi Dataset"
# Try official NYC taxi zone lookup first, then fallback to custom zones
ZONES_LOOKUP_PATH = PROJECT_ROOT.parent / "Taxi Dataset" / "taxi_zone_lookup.csv"
if not ZONES_LOOKUP_PATH.exists():
    ZONES_LOOKUP_PATH = PROJECT_ROOT / "Taxi Dataset" / "taxi_zone_lookup.csv"
ZONES_DATA_PATH = PROJECT_ROOT / "data" / "zones.csv"
OUTPUT_DIR = PROJECT_ROOT / "output"

# MongoDB configuration (to be set by user)
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb+srv://username:password@cluster.mongodb.net/")
MONGODB_DATABASE = "taxi_analysis"
MONGODB_COLLECTIONS = {
    "trips_by_hour": "trips_by_hour",
    "trips_by_day": "trips_by_day",
    "zones_activity": "zones_activity",
    "trip_duration_stats": "trip_duration_stats",
    "revenue_analysis": "revenue_analysis"
}

# Spark configuration
SPARK_CONFIG = {
    "spark.app.name": "UrbanMobilityAnalysis",
    "spark.master": "local[*]",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.driver.memory": "4g",
    "spark.executor.memory": "4g",
    # Hadoop security settings to avoid Java 17+ issues
    "spark.hadoop.hadoop.security.authentication": "simple",
    "spark.hadoop.hadoop.security.authorization": "false"
}

# Data quality thresholds
MIN_TRIP_DISTANCE_KM = 0.1  # Minimum valid trip distance in km
MAX_TRIP_DISTANCE_KM = 200  # Maximum valid trip distance in km
MIN_TRIP_DURATION_MIN = 1  # Minimum valid trip duration in minutes
MAX_TRIP_DURATION_MIN = 180  # Maximum valid trip duration in minutes
MIN_FARE_AMOUNT = 0  # Minimum valid fare amount
MAX_FARE_AMOUNT = 1000  # Maximum valid fare amount

# NYC approximate boundaries (for zone creation if needed)
NYC_BOUNDS = {
    "min_longitude": -74.05,
    "max_longitude": -73.75,
    "min_latitude": 40.55,
    "max_latitude": 40.95
}


