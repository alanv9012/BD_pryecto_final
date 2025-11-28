"""
MongoDB operations for storing analysis results
"""

from pyspark.sql import DataFrame
from pymongo import MongoClient
import pandas as pd
import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)


def get_mongodb_client(uri: str):
    """
    Create MongoDB client connection
    
    Args:
        uri: MongoDB connection URI
        
    Returns:
        MongoClient instance
    """
    try:
        client = MongoClient(uri)
        # Test connection
        client.admin.command('ping')
        logger.info("Successfully connected to MongoDB")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise


def save_dataframe_to_mongodb(
    df: DataFrame,
    uri: str,
    database: str,
    collection: str,
    mode: str = "overwrite"
) -> None:
    """
    Save Spark DataFrame to MongoDB collection
    
    Args:
        df: Spark DataFrame to save
        uri: MongoDB connection URI
        database: Database name
        collection: Collection name
        mode: Write mode ("overwrite" or "append")
    """
    logger.info(f"Saving DataFrame to MongoDB: {database}.{collection}")
    
    try:
        # Convert Spark DataFrame to Pandas (for smaller datasets)
        # For larger datasets, consider using MongoDB Spark Connector
        pandas_df = df.toPandas()
        
        # Connect to MongoDB
        client = get_mongodb_client(uri)
        db = client[database]
        coll = db[collection]
        
        # Convert DataFrame to dictionary records
        records = pandas_df.to_dict('records')
        
        if mode == "overwrite":
            # Clear existing collection
            coll.delete_many({})
            logger.info(f"Cleared existing data in {collection}")
        
        # Insert records
        if records:
            coll.insert_many(records)
            logger.info(f"Inserted {len(records)} records into {collection}")
        else:
            logger.warning(f"No records to insert into {collection}")
        
        client.close()
        
    except Exception as e:
        logger.error(f"Error saving to MongoDB: {e}")
        raise


def save_analysis_results(
    results: Dict[str, Optional[DataFrame]],
    uri: str,
    database: str,
    collections: Dict[str, str]
) -> None:
    """
    Save all analysis results to MongoDB
    
    Args:
        results: Dictionary of analysis results (DataFrames)
        uri: MongoDB connection URI
        database: Database name
        collections: Dictionary mapping result keys to collection names
    """
    logger.info("Saving all analysis results to MongoDB")
    
    for result_key, df in results.items():
        if df is not None and result_key in collections:
            collection_name = collections[result_key]
            try:
                save_dataframe_to_mongodb(
                    df, uri, database, collection_name, mode="overwrite"
                )
            except Exception as e:
                logger.error(f"Failed to save {result_key} to MongoDB: {e}")


def export_for_powerbi(df: DataFrame, output_path: str, format: str = "csv") -> None:
    """
    Export DataFrame to file format suitable for Power BI
    
    Args:
        df: Spark DataFrame to export
        output_path: Path to save the file
        format: Export format ("csv" or "json")
    """
    logger.info(f"Exporting DataFrame to {output_path} in {format} format")
    
    try:
        if format.lower() == "csv":
            pandas_df = df.toPandas()
            pandas_df.to_csv(output_path, index=False)
        elif format.lower() == "json":
            pandas_df = df.toPandas()
            pandas_df.to_json(output_path, orient="records", date_format="iso")
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        logger.info(f"Successfully exported to {output_path}")
    except Exception as e:
        logger.error(f"Error exporting to {output_path}: {e}")
        raise


