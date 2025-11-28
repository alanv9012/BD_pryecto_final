# Urban Mobility Pattern Analysis - Project Report

## 1. Objectives

This project aims to:
- Apply Apache Spark (PySpark) for processing and analyzing urban mobility data
- Identify mobility patterns including:
  - Peak hours and demand patterns
  - Most active zones (origin and destination)
  - Average trip duration and distance by time and location
  - Revenue analysis by zone, day, and payment type
- Develop a complete data analysis workflow: ingestion, cleaning, transformation, analysis, and visualization

## 2. Problem Description

A urban transportation company (taxi service) wants to understand mobility patterns in the city to:
- Identify peak demand hours for better resource allocation
- Understand which zones generate the most trips (origin and destination)
- Analyze trip duration patterns by time and location
- Evaluate revenue generation by zone and day of week
- Make data-driven decisions for fleet management and pricing strategies

## 3. Dataset Description

### 3.1 Data Source
- **Dataset**: New York City Taxi & Limousine Commission (TLC) Trip Record Data
- **Format**: Parquet files
- **Period**: January 2025 - October 2025
- **Files**: 10 monthly parquet files

### 3.2 Data Schema
The taxi trip data includes:
- **trip_id**: Unique identifier for each trip
- **pickup_datetime**: Date and time of trip start
- **dropoff_datetime**: Date and time of trip end
- **pickup_longitude/latitude**: Geographic coordinates of trip origin
- **dropoff_longitude/latitude**: Geographic coordinates of trip destination
- **passenger_count**: Number of passengers
- **trip_distance_km**: Distance traveled in kilometers
- **fare_amount**: Cost of the trip
- **payment_type**: Method of payment (credit card, cash, etc.)

### 3.3 Zones Dataset
Created a zones dataset with 20 zones covering:
- Manhattan (Central, North, South, East, West)
- Brooklyn (Downtown, North, South)
- Queens (Midtown, Airport areas)
- Bronx (Central, South)
- Staten Island
- Airport zones (JFK, LaGuardia, Newark)

Each zone includes:
- **zone_id**: Unique identifier
- **zone_name**: Descriptive name
- **min/max_longitude/latitude**: Geographic boundaries

## 4. Architecture and Technology Stack

### 4.1 Components
1. **Data Source**: NYC Taxi parquet files
2. **Processing Engine**: Apache Spark (PySpark) - Local mode
3. **Development Environment**: Jupyter Notebook
4. **Storage**: MongoDB Atlas (free tier)
5. **Visualization**: Power BI

### 4.2 Data Flow
```
Parquet Files → Spark Processing → Cleaned Data → Zone Enrichment 
→ Analysis → MongoDB Storage + CSV Export → Power BI Visualization
```

## 5. Data Cleaning and Preprocessing Decisions

### 5.1 Cleaning Steps
1. **Null Value Removal**: Removed records with null values in key fields:
   - Pickup/dropoff datetime
   - Trip distance
   - Fare amount

2. **Type Conversions**:
   - Converted datetime strings to timestamp format
   - Ensured numeric fields (distance, fare) are double type

3. **Derived Columns Created**:
   - `trip_duration_min`: Calculated from pickup and dropoff times
   - `hour_of_day`: Extracted from pickup datetime (0-23)
   - `day_of_week`: Day of week (1=Sunday, 7=Saturday)
   - `day_name`: Human-readable day name

4. **Outlier Filtering**:
   - Trip distance: 0.1 km - 200 km
   - Trip duration: 1 - 180 minutes
   - Fare amount: $0 - $1,000
   - Removed trips with distance = 0 but duration > 30 minutes

### 5.2 Data Quality Metrics
- **Initial Records**: [To be filled after running analysis]
- **Records After Cleaning**: [To be filled after running analysis]
- **Data Quality Rate**: [To be calculated]

## 6. Data Enrichment

### 6.1 Zone Assignment
- Loaded zones dataset from CSV file
- Assigned pickup and dropoff zones based on coordinate matching
- Used bounding box approach: coordinates within zone boundaries
- Handled cases where coordinates fall outside defined zones

## 7. Analysis Results

### 7.1 Demand by Hour of Day
**Key Findings**:
- Peak hour: [To be filled]
- Lowest demand hour: [To be filled]
- Average trips per hour: [To be calculated]

**Insights**: [To be added after analysis]

### 7.2 Demand by Day of Week
**Key Findings**:
- Busiest day: [To be filled]
- Quietest day: [To be filled]
- Weekday vs Weekend comparison: [To be added]

**Insights**: [To be added after analysis]

### 7.3 Zone Activity
**Top 10 Origin Zones**:
1. [To be filled]
2. [To be filled]
...

**Top 10 Destination Zones**:
1. [To be filled]
2. [To be filled]
...

**Revenue by Zone**: [To be added]

### 7.4 Trip Duration and Distance
**Average Duration by Hour**: [To be added]
**Average Distance by Zone**: [To be added]
**Patterns Identified**: [To be added]

### 7.5 Revenue Analysis
**Total Revenue**: $[To be filled]
**Revenue by Payment Type**:
- Credit Card: $[To be filled]
- Cash: $[To be filled]
- Other: $[To be filled]

**Average Revenue per Trip**: $[To be calculated]

## 8. Storage and Visualization

### 8.1 MongoDB Collections
Data stored in MongoDB Atlas with the following collections:
- `trips_by_hour`: Hourly aggregated statistics
- `trips_by_day`: Daily aggregated statistics
- `zones_activity`: Zone-level activity metrics
- `trip_duration_stats`: Duration and distance statistics
- `revenue_analysis`: Revenue breakdown by payment type

### 8.2 Power BI Exports
CSV files exported for Power BI visualization:
- `hourly_demand.csv`: Hourly trip and revenue data
- `daily_demand.csv`: Daily trip and revenue data
- `zone_activity.csv`: Zone activity metrics
- `revenue_analysis.csv`: Revenue by payment type

### 8.3 Visualization Recommendations
**Recommended Power BI Visualizations**:
1. **Time Series Chart**: Hourly demand curve showing peak hours
2. **Bar Chart**: Daily demand comparison (weekday vs weekend)
3. **Map Visualization**: Heat map showing zone activity
4. **Pie Chart**: Revenue distribution by payment type
5. **Table**: Top zones by trips and revenue

## 9. Conclusions and Recommendations

### 9.1 Key Insights
[To be filled after analysis completion]

### 9.2 Business Recommendations
1. **Resource Allocation**: [To be added]
2. **Pricing Strategy**: [To be added]
3. **Fleet Management**: [To be added]
4. **Zone-Specific Strategies**: [To be added]

### 9.3 Technical Learnings
- Apache Spark efficiently processes large-scale taxi data
- Zone assignment using coordinate matching works well for geographic analysis
- MongoDB provides flexible storage for aggregated results
- Power BI integration enables interactive visualizations

## 10. Future Enhancements

1. Real-time data processing pipeline
2. Machine learning models for demand forecasting
3. Integration with weather data for pattern analysis
4. Driver performance analysis
5. Route optimization recommendations

## 11. References

- NYC Taxi & Limousine Commission: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- Apache Spark Documentation: https://spark.apache.org/docs/latest/
- MongoDB Atlas: https://www.mongodb.com/cloud/atlas
- Power BI Documentation: https://docs.microsoft.com/power-bi/

---

**Project Completion Date**: [To be filled]
**Author**: [Your Name]
**Version**: 1.0


