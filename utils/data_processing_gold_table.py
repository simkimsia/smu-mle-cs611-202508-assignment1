import os
import glob
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pprint
import pyspark
import pyspark.sql.functions as F
import argparse

from pyspark.sql.functions import col
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType


def process_labels_gold_table(snapshot_date_str, silver_loan_daily_directory, gold_label_store_directory, spark, dpd, mob):
    
    # prepare arguments
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d")
    
    # connect to bronze table
    partition_name = "silver_loan_daily_" + snapshot_date_str.replace('-','_') + '.parquet'
    filepath = silver_loan_daily_directory + partition_name
    df = spark.read.parquet(filepath)
    print('loaded from:', filepath, 'row count:', df.count())

    # get customer at mob
    df = df.filter(col("mob") == mob)

    # get label
    df = df.withColumn("label", F.when(col("dpd") >= dpd, 1).otherwise(0).cast(IntegerType()))
    df = df.withColumn("label_def", F.lit(str(dpd)+'dpd_'+str(mob)+'mob').cast(StringType()))

    # select columns to save
    df = df.select("loan_id", "Customer_ID", "label", "label_def", "snapshot_date")

    # save gold table - IRL connect to database to write
    partition_name = "gold_label_store_" + snapshot_date_str.replace('-','_') + '.parquet'
    filepath = gold_label_store_directory + partition_name
    df.write.mode("overwrite").parquet(filepath)
    # df.toPandas().to_parquet(filepath,
    #           compression='gzip')
    print('saved to:', filepath)
    
    return df


def process_features_gold_table(snapshot_date_str, silver_directories, gold_feature_store_directory, spark):
    """
    Create ML-ready feature store by joining silver tables on user_id and snapshot_date

    Args:
        snapshot_date_str: Date string in YYYY-MM-DD format
        silver_directories: Dict with keys 'attributes', 'financials', 'clickstream', 'loan_daily'
        gold_feature_store_directory: Output directory for feature store
        spark: SparkSession
    """

    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d")
    partition_suffix = snapshot_date_str.replace('-', '_')

    # Load silver tables for this snapshot date
    silver_files = {
        'loan_daily': f"{silver_directories['loan_daily']}silver_loan_daily_{partition_suffix}.parquet",
        'attributes': f"{silver_directories['attributes']}silver_attributes_{partition_suffix}.parquet",
        'financials': f"{silver_directories['financials']}silver_financials_{partition_suffix}.parquet",
        'clickstream': f"{silver_directories['clickstream']}silver_clickstream_{partition_suffix}.parquet"
    }

    # Check which files exist
    available_tables = {}
    for table_name, filepath in silver_files.items():
        if os.path.exists(filepath):
            available_tables[table_name] = spark.read.parquet(filepath)
            print(f'Loaded {table_name}: {filepath}, rows: {available_tables[table_name].count()}')
        else:
            print(f'Warning: {table_name} file not found: {filepath}')

    # Start with loan_daily as base (since it has the prediction scenarios)
    if 'loan_daily' not in available_tables:
        print(f"Error: loan_daily silver table required but not found")
        return None

    df_features = available_tables['loan_daily'].select("loan_id", "Customer_ID", "snapshot_date", "mob")

    # Join with other tables on Customer_ID
    for table_name in ['attributes', 'financials', 'clickstream']:
        if table_name in available_tables:
            # Get existing columns in df_features to avoid conflicts
            existing_columns = set(df_features.columns)
            other_table = available_tables[table_name]

            # Drop columns that already exist (except Customer_ID which is the join key)
            columns_to_drop = [col for col in other_table.columns if col in existing_columns and col != "Customer_ID"]
            if columns_to_drop:
                print(f'Dropping duplicate columns from {table_name}: {columns_to_drop}')
                other_table = other_table.drop(*columns_to_drop)

            df_features = df_features.join(
                other_table,
                on="Customer_ID",
                how="left"
            )
            print(f'Joined {table_name} features')

    # Add feature metadata
    df_features = df_features.withColumn("feature_snapshot_date", F.lit(snapshot_date_str).cast(StringType()))

    # Save feature store
    partition_name = f"gold_feature_store_{partition_suffix}.parquet"
    filepath = gold_feature_store_directory + partition_name
    df_features.write.mode("overwrite").parquet(filepath)
    print(f'Feature store saved to: {filepath}, rows: {df_features.count()}')

    return df_features