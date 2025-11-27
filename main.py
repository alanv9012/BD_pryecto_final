"""
Urban Mobility Pattern Analysis - Main Script

This script runs the complete data analysis pipeline:
1. Data ingestion and exploration
2. Data cleaning and preprocessing
3. Zone enrichment
4. Exploratory analysis
5. MongoDB storage (optional)
6. CSV export for Power BI
"""

import sys
import os
from pathlib import Path
import logging
from datetime import datetime

# Fix for Java 17+ compatibility - MUST be set before importing PySpark
# These options allow Spark to work with newer Java versions
java_opts = [
    '--add-opens=java.base/java.lang=ALL-UNNAMED',
    '--add-opens=java.base/java.lang.invoke=ALL-UNNAMED',
    '--add-opens=java.base/java.lang.reflect=ALL-UNNAMED',
    '--add-opens=java.base/java.io=ALL-UNNAMED',
    '--add-opens=java.base/java.net=ALL-UNNAMED',
    '--add-opens=java.base/java.nio=ALL-UNNAMED',
    '--add-opens=java.base/java.util=ALL-UNNAMED',
    '--add-opens=java.base/java.util.concurrent=ALL-UNNAMED',
    '--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED',
    '--add-opens=java.base/sun.nio.ch=ALL-UNNAMED',
    '--add-opens=java.base/sun.nio.cs=ALL-UNNAMED',
    '--add-opens=java.base/sun.security.action=ALL-UNNAMED',
    '--add-opens=java.base/sun.util.calendar=ALL-UNNAMED',
    '--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED',
    '--add-opens=java.base/javax.security.auth=ALL-UNNAMED',
    '--enable-native-access=ALL-UNNAMED',
    '-Dio.netty.tryReflectionSetAccessible=true',
    '-Dhadoop.security.authentication=simple',
    '-Dhadoop.security.authorization=false'
]

# Set environment variables before PySpark is imported
os.environ['JAVA_OPTS'] = ' '.join(java_opts)
os.environ['_JAVA_OPTIONS'] = ' '.join(java_opts)

# Add src directory to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'analysis_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Import project modules
from src.config import *
from src.data_processing import *
from src.zone_mapping import *
from src.analysis import *
from src.mongodb_operations import *
from pyspark.sql.functions import desc, sum as spark_sum, col

def print_section(title):
    """Print a formatted section header"""
    print("\n" + "="*80)
    print(f"  {title}")
    print("="*80 + "\n")

def main():
    """Main execution function"""
    
    print_section("URBAN MOBILITY PATTERN ANALYSIS")
    print("Starting analysis pipeline...")
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    try:
        # ====================================================================
        # STEP 1: Initialize Spark Session
        # ====================================================================
        print_section("STEP 1: Initializing Spark Session")
        spark = create_spark_session()
        print(f"✓ Spark Version: {spark.version}")
        print(f"✓ Spark Master: {spark.sparkContext.master}")
        logger.info("Spark session initialized successfully")
        
        # ====================================================================
        # STEP 2: Data Ingestion and Initial Exploration
        # ====================================================================
        print_section("STEP 2: Data Ingestion and Exploration")
        
        taxi_data_path = str(TAXI_DATA_DIR)
        print(f"Loading data from: {taxi_data_path}")
        
        df_raw = load_taxi_data(spark, taxi_data_path)
        
        print(f"\n=== Data Schema ===")
        df_raw.printSchema()
        
        print(f"\n=== Basic Statistics ===")
        print(f"Total records: {df_raw.count():,}")
        print(f"Total columns: {len(df_raw.columns)}")
        print(f"Column names: {df_raw.columns}")
        
        # Show sample data
        print(f"\n=== Sample Data (first 5 rows) ===")
        df_raw.show(5, truncate=False)
        
        # Check for null values
        print(f"\n=== Null Value Analysis ===")
        from pyspark.sql.functions import isnan, isnull, when, count
        from pyspark.sql.types import NumericType
        
        # Get column types to determine which columns can use isnan()
        schema = df_raw.schema
        column_types = {field.name: field.dataType for field in schema.fields}
        
        # Build null count expressions - only use isnan() for numeric types
        null_expressions = []
        for col_name in df_raw.columns:
            col_type = column_types.get(col_name)
            # For numeric types, check both isnull and isnan
            if isinstance(col_type, NumericType):
                null_expressions.append(count(when(isnull(col(col_name)) | isnan(col(col_name)), col(col_name))).alias(col_name))
            else:
                # For non-numeric types (timestamps, strings), only check isnull
                null_expressions.append(count(when(isnull(col(col_name)), col(col_name))).alias(col_name))
        
        null_counts = df_raw.select(null_expressions)
        null_counts.show(vertical=True)
        
        # ====================================================================
        # STEP 3: Data Cleaning and Preprocessing
        # ====================================================================
        print_section("STEP 3: Data Cleaning and Preprocessing")
        print("Starting data preprocessing...")
        
        df_cleaned = preprocess_taxi_data(spark, taxi_data_path)
        
        print(f"\n✓ Preprocessing complete!")
        print(f"✓ Final record count: {df_cleaned.count():,}")
        
        print(f"\n=== Cleaned Data Schema ===")
        df_cleaned.printSchema()
        
        print(f"\n=== Sample Cleaned Data ===")
        sample_cols = ["pickup_datetime", "dropoff_datetime", "trip_duration_min",
                      "hour_of_day", "day_of_week", "day_name",
                      "trip_distance_km", "fare_amount", "passenger_count"]
        available_cols = [c for c in sample_cols if c in df_cleaned.columns]
        df_cleaned.select(available_cols).show(10, truncate=False)
        
        # ====================================================================
        # STEP 4: Data Enrichment - Zone Assignment
        # ====================================================================
        print_section("STEP 4: Zone Assignment")
        
        # Try to use official NYC zone lookup table first
        from src.config import ZONES_LOOKUP_PATH
        zones_df = None
        
        if ZONES_LOOKUP_PATH.exists():
            print(f"Loading official NYC zone lookup from: {ZONES_LOOKUP_PATH}")
            try:
                from src.zone_mapping import load_nyc_zone_lookup
                zones_df = load_nyc_zone_lookup(spark, str(ZONES_LOOKUP_PATH))
                print(f"✓ Loaded {zones_df.count()} official NYC zones")
                zones_df.show(10, truncate=False)
            except Exception as e:
                logger.warning(f"Error loading official zone lookup: {e}")
                zones_df = None
        
        # Fallback to custom zones if lookup table not available
        if zones_df is None:
            zones_path = str(ZONES_DATA_PATH)
            print(f"Loading custom zones from: {zones_path}")
            try:
                zones_df = load_zones(spark, zones_path)
                print(f"✓ Loaded {zones_df.count()} custom zones")
                zones_df.show(truncate=False)
            except Exception as e:
                logger.warning(f"Error loading zones file: {e}")
                print("Creating zones from data...")
                zones_df = create_zones_from_data(df_cleaned, num_zones=20)
                zones_df.show(truncate=False)
        
        print("\nEnriching trips with zone information...")
        df_enriched = enrich_with_zones(df_cleaned, zones_df)
        
        print("✓ Zone enrichment complete!")
        if 'pickup_zone_name' in df_enriched.columns:
            pickup_count = df_enriched.filter(col('pickup_zone_name').isNotNull()).count()
            print(f"✓ Records with pickup zone: {pickup_count:,}")
        if 'dropoff_zone_name' in df_enriched.columns:
            dropoff_count = df_enriched.filter(col('dropoff_zone_name').isNotNull()).count()
            print(f"✓ Records with dropoff zone: {dropoff_count:,}")
        
        print(f"\n=== Sample Enriched Data ===")
        enriched_cols = ["pickup_datetime", "hour_of_day", "day_name",
                        "pickup_zone_name", "dropoff_zone_name",
                        "trip_duration_min", "trip_distance_km", "fare_amount"]
        available_enriched = [c for c in enriched_cols if c in df_enriched.columns]
        if available_enriched:
            df_enriched.select(available_enriched).show(10, truncate=False)
        
        # ====================================================================
        # STEP 5: Exploratory Data Analysis
        # ====================================================================
        print_section("STEP 5: Exploratory Data Analysis")
        
        # 5.1 Demand by Hour
        print("\n--- 5.1 Demand by Hour of Day ---")
        hourly_demand = analyze_demand_by_hour(df_enriched)
        
        if hourly_demand:
            print("\n=== Hourly Demand Analysis ===")
            hourly_demand.show(24, truncate=False)
            
            # Find peak hours
            peak_hour = hourly_demand.orderBy(desc("total_trips")).first()
            print(f"\n✓ Peak hour: {peak_hour['hour_of_day']}:00 with {peak_hour['total_trips']:,} trips")
        else:
            print("⚠ Hourly analysis not available (missing hour_of_day column)")
        
        # 5.2 Demand by Day
        print("\n--- 5.2 Demand by Day of Week ---")
        daily_demand = analyze_demand_by_day(df_enriched)
        
        if daily_demand:
            print("\n=== Daily Demand Analysis ===")
            daily_demand.show(truncate=False)
            
            # Compare weekdays vs weekends
            weekday_weekend = daily_demand.groupBy("is_weekend").agg(
                spark_sum("total_trips").alias("total_trips"),
                spark_sum("total_revenue").alias("total_revenue")
            )
            print("\n=== Weekday vs Weekend Comparison ===")
            weekday_weekend.show(truncate=False)
        else:
            print("⚠ Daily analysis not available (missing day_of_week column)")
        
        # 5.3 Zone Activity
        print("\n--- 5.3 Zone Activity Analysis ---")
        zone_results = analyze_zone_activity(df_enriched)
        
        if zone_results.get("top_origin_zones"):
            print("\n=== Top 10 Origin Zones ===")
            zone_results["top_origin_zones"].show(truncate=False)
        
        if zone_results.get("top_destination_zones"):
            print("\n=== Top 10 Destination Zones ===")
            zone_results["top_destination_zones"].show(truncate=False)
        
        if zone_results.get("combined_zone_activity"):
            print("\n=== Top 20 Zones by Total Activity ===")
            zone_results["combined_zone_activity"].show(truncate=False)
        
        # 5.4 Trip Duration and Distance
        print("\n--- 5.4 Trip Duration and Distance Analysis ---")
        duration_distance_stats = analyze_trip_duration_distance(df_enriched)
        
        for stat_type, stats_df in duration_distance_stats:
            print(f"\n=== Duration and Distance Statistics ({stat_type}) ===")
            stats_df.show(truncate=False)
        
        # 5.5 Revenue Analysis
        print("\n--- 5.5 Revenue and Payment Type Analysis ---")
        revenue_analysis = analyze_revenue_payment(df_enriched)
        
        if revenue_analysis:
            print("\n=== Revenue Analysis by Payment Type ===")
            revenue_analysis.show(truncate=False)
            
            # Calculate total revenue
            total_revenue = revenue_analysis.agg(spark_sum("total_revenue").alias("total_revenue")).collect()[0]["total_revenue"]
            print(f"\n✓ Total Revenue: ${total_revenue:,.2f}")
        else:
            print("⚠ Revenue analysis not available (missing payment_type column)")
        
        # ====================================================================
        # STEP 6: Data Storage - MongoDB (Optional)
        # ====================================================================
        print_section("STEP 6: MongoDB Storage (Optional)")
        
        # Prepare all analysis results for MongoDB
        analysis_results = {
            "trips_by_hour": hourly_demand,
            "trips_by_day": daily_demand,
            "zones_activity": zone_results.get("combined_zone_activity"),
            "revenue_analysis": revenue_analysis
        }
        
        # Save to MongoDB (only if URI is configured)
        if MONGODB_URI and MONGODB_URI != "mongodb+srv://username:password@cluster.mongodb.net/":
            try:
                print("Saving analysis results to MongoDB...")
                save_analysis_results(
                    analysis_results,
                    MONGODB_URI,
                    MONGODB_DATABASE,
                    MONGODB_COLLECTIONS
                )
                print("✓ Successfully saved all results to MongoDB!")
            except Exception as e:
                logger.error(f"Error saving to MongoDB: {e}")
                print(f"⚠ Error saving to MongoDB: {e}")
                print("  Please check your MongoDB connection string and network access.")
        else:
            print("⚠ MongoDB URI not configured. Skipping MongoDB storage.")
            print("  To enable MongoDB storage, set MONGODB_URI environment variable or update src/config.py")
        
        # ====================================================================
        # STEP 7: Export Data for Power BI
        # ====================================================================
        print_section("STEP 7: Export Data for Power BI")
        
        # Ensure output directory exists
        OUTPUT_DIR.mkdir(exist_ok=True)
        print(f"Output directory: {OUTPUT_DIR}")
        
        # Export all analysis results to CSV for Power BI
        print("\nExporting data for Power BI...")
        exports = []
        
        if hourly_demand:
            output_path = str(OUTPUT_DIR / "hourly_demand.csv")
            export_for_powerbi(hourly_demand, output_path)
            exports.append("hourly_demand.csv")
            print(f"  ✓ Exported hourly_demand.csv")
        
        if daily_demand:
            output_path = str(OUTPUT_DIR / "daily_demand.csv")
            export_for_powerbi(daily_demand, output_path)
            exports.append("daily_demand.csv")
            print(f"  ✓ Exported daily_demand.csv")
        
        if zone_results.get("combined_zone_activity"):
            output_path = str(OUTPUT_DIR / "zone_activity.csv")
            export_for_powerbi(zone_results["combined_zone_activity"], output_path)
            exports.append("zone_activity.csv")
            print(f"  ✓ Exported zone_activity.csv")
        
        if revenue_analysis:
            output_path = str(OUTPUT_DIR / "revenue_analysis.csv")
            export_for_powerbi(revenue_analysis, output_path)
            exports.append("revenue_analysis.csv")
            print(f"  ✓ Exported revenue_analysis.csv")
        
        print(f"\n✓ All exports complete! {len(exports)} files exported.")
        print(f"  Files are ready for Power BI import in: {OUTPUT_DIR}")
        
        # ====================================================================
        # STEP 8: Summary and Key Findings
        # ====================================================================
        print_section("STEP 8: Summary and Key Findings")
        
        print(f"Total trips analyzed: {df_enriched.count():,}\n")
        
        if hourly_demand:
            peak = hourly_demand.orderBy(desc("total_trips")).first()
            print(f"✓ Peak hour: {peak['hour_of_day']}:00 ({peak['total_trips']:,} trips)")
        
        if daily_demand:
            busiest_day = daily_demand.orderBy(desc("total_trips")).first()
            print(f"✓ Busiest day: {busiest_day['day_name']} ({busiest_day['total_trips']:,} trips)")
        
        if zone_results.get("combined_zone_activity"):
            top_zone = zone_results["combined_zone_activity"].first()
            print(f"✓ Most active zone: {top_zone['zone_name']} ({top_zone['total_activity']:,} trips)")
        
        if revenue_analysis:
            total_rev = revenue_analysis.agg(spark_sum("total_revenue").alias("total")).collect()[0]["total"]
            print(f"✓ Total revenue: ${total_rev:,.2f}")
        
        print("\n" + "="*80)
        print("  ANALYSIS COMPLETE!")
        print("="*80)
        print("\nNext steps:")
        print("1. Review exported CSV files in the output/ directory")
        print("2. Import data into Power BI for visualization")
        print("3. Review log file for detailed execution information")
        if not (MONGODB_URI and MONGODB_URI != "mongodb+srv://username:password@cluster.mongodb.net/"):
            print("4. Optionally configure MongoDB for persistent storage")
        print()
        
        # ====================================================================
        # Cleanup
        # ====================================================================
        print_section("Cleaning Up")
        spark.stop()
        print("✓ Spark session stopped.")
        logger.info("Analysis pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Error in analysis pipeline: {e}", exc_info=True)
        print(f"\n❌ ERROR: {e}")
        print("\nCheck the log file for detailed error information.")
        raise

if __name__ == "__main__":
    main()

