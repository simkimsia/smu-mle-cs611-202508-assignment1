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

    # ============================================================================
    # IMPORTANT: Filter to mob=0 (loan application time) to prevent data leakage
    # - Features should only use information available at application time
    # - This ensures we don't include future information (e.g., payment history)
    # - Labels are created at mob=6 (outcome), but features must be at mob=0 (prediction time)
    # ============================================================================
    df_features = available_tables['loan_daily'].filter(col("mob") == 0).select("loan_id", "Customer_ID", "snapshot_date", "mob")

    # Join with other tables on Customer_ID
    # Process attributes and financials (static/single snapshot per customer)
    for table_name in ['attributes', 'financials']:
        if table_name in available_tables:
            other_table = available_tables[table_name]

            # Skip if table is empty
            if other_table.count() == 0:
                print(f'Skipping {table_name} - table is empty')
                continue

            # Get existing columns in df_features to avoid conflicts
            existing_columns = set(df_features.columns)

            # ============================================================================
            # FEATURE SELECTION: Drop non-predictive identifiers and raw categoricals
            # - SSN: Unique identifier with no predictive power, privacy risk
            # - valid_ssn: Silver validation flag for SSN (not needed in gold)
            # - Name: No predictive value, high cardinality, privacy concerns
            # - Payment_of_Min_Amount: Raw categorical (replaced by payment_min_yes/payment_min_nm)
            # - valid_payment_of_min_amount: Silver validation flag (not needed in gold)
            # - Credit_Mix: Raw categorical (replaced by credit_mix_good/credit_mix_standard/credit_mix_bad)
            # - valid_credit_mix: Silver validation flag (not needed in gold)
            # - Credit_History_Age: Raw string format (replaced by Credit_History_Age_Months)
            # - valid_credit_history_age: Silver validation flag (not needed in gold)
            # - Type_of_Loan: Raw comma-separated string (replaced by has_* binary features)
            # - valid_type_of_loan: Silver validation flag (not needed in gold)
            # ============================================================================
            non_predictive_cols = ['SSN', 'valid_ssn', 'Name', 'Payment_of_Min_Amount', 'valid_payment_of_min_amount', 'Credit_Mix', 'valid_credit_mix', 'Credit_History_Age', 'valid_credit_history_age', 'Type_of_Loan', 'valid_type_of_loan']
            cols_to_exclude = [c for c in non_predictive_cols if c in other_table.columns]
            if cols_to_exclude:
                print(f'Excluding non-predictive columns from {table_name}: {cols_to_exclude}')
                other_table = other_table.drop(*cols_to_exclude)

            # ============================================================================
            # FEATURE ENGINEERING: PMET Classification
            # - Reduce high cardinality of Occupation into binary PMET indicator
            # - PMET = Professionals, Managers, Executives, Technicians
            # - Preserves predictive signal (income stability, education) while reducing complexity
            # ============================================================================
            if 'Occupation' in other_table.columns:
                # Define PMET occupations based on formal education, professional certification, stable employment
                PMET_occupations = [
                    'LAWYER', 'DOCTOR', 'ACCOUNTANT', 'MANAGER', 'SCIENTIST',
                    'ARCHITECT', 'TEACHER', 'ENGINEER', 'DEVELOPER',
                    'MEDIA_MANAGER', 'JOURNALIST'
                ]

                # Create binary PMET indicator
                # Note: Occupation is already uppercased in silver layer
                other_table = other_table.withColumn(
                    'is_PMET',
                    F.when(
                        col('Occupation').isNotNull() & col('Occupation').isin(PMET_occupations),
                        1
                    ).otherwise(0)
                )
                print(f'Created is_PMET feature from Occupation (PMET occupations: {len(PMET_occupations)})')

            # ============================================================================
            # FEATURE ENGINEERING: Payment_Behavior Decomposition
            # - Reduce high cardinality categorical (6 unique values) into 2 engineered features
            # - spending_level: Binary (0=Low_spent, 1=High_spent)
            # - value_size: Ordinal (0=Small, 1=Medium, 2=Large)
            # - Preserves predictive signal while reducing dimensionality
            # - Avoids sparse one-hot encoding (6 columns → 2 columns)
            # - Note: Payment_Behavior is uppercased in silver layer
            # ============================================================================
            if 'Payment_Behavior' in other_table.columns:
                # Extract spending level (Low vs High)
                # Payment_Behavior values are uppercased in silver: HIGH_SPENT_MEDIUM_VALUE_PAYMENTS
                other_table = other_table.withColumn(
                    'spending_level',
                    F.when(col('Payment_Behavior').contains('HIGH_SPENT'), 1).otherwise(0)
                )

                # Extract payment value size (Small=0, Medium=1, Large=2)
                other_table = other_table.withColumn(
                    'value_size',
                    F.when(col('Payment_Behavior').contains('LARGE_VALUE'), 2)
                     .when(col('Payment_Behavior').contains('MEDIUM_VALUE'), 1)
                     .otherwise(0)
                )

                print(f'Created spending_level (binary) and value_size (ordinal) from Payment_Behavior')

            # ============================================================================
            # FEATURE ENGINEERING: Payment_of_Min_Amount One-Hot Encoding
            # - Convert categorical Payment_of_Min_Amount (YES/NO/NM) into binary indicators
            # - payment_min_yes: 1 if pays minimum, 0 otherwise
            # - payment_min_nm: 1 if no minimum required, 0 otherwise
            # - NO becomes reference category (both indicators = 0)
            # - Avoids ordinal assumption (no clear ordering between YES and NM)
            # - Note: Payment_of_Min_Amount is uppercased in silver layer
            # ============================================================================
            if 'Payment_of_Min_Amount' in other_table.columns:
                # Create binary indicator for YES (pays minimum)
                other_table = other_table.withColumn(
                    'payment_min_yes',
                    F.when(col('Payment_of_Min_Amount') == 'YES', 1).otherwise(0)
                )

                # Create binary indicator for NM (no minimum required)
                other_table = other_table.withColumn(
                    'payment_min_nm',
                    F.when(col('Payment_of_Min_Amount') == 'NM', 1).otherwise(0)
                )

                print(f'Created payment_min_yes and payment_min_nm (one-hot encoding) from Payment_of_Min_Amount')

            # ============================================================================
            # FEATURE ENGINEERING: Credit_Mix One-Hot Encoding
            # - Convert categorical Credit_Mix (GOOD/STANDARD/BAD/missing) into binary indicators
            # - credit_mix_good: 1 if credit mix is good, 0 otherwise
            # - credit_mix_standard: 1 if credit mix is standard, 0 otherwise
            # - credit_mix_bad: 1 if credit mix is bad, 0 otherwise
            # - Missing/null (~20% of data) becomes reference category (all three = 0)
            # - Treats missing as legitimate category with potential predictive signal
            # - Note: Credit_Mix is uppercased in silver layer, "_" converted to null
            # ============================================================================
            if 'Credit_Mix' in other_table.columns:
                # Create binary indicator for GOOD
                other_table = other_table.withColumn(
                    'credit_mix_good',
                    F.when(col('Credit_Mix') == 'GOOD', 1).otherwise(0)
                )

                # Create binary indicator for STANDARD
                other_table = other_table.withColumn(
                    'credit_mix_standard',
                    F.when(col('Credit_Mix') == 'STANDARD', 1).otherwise(0)
                )

                # Create binary indicator for BAD
                other_table = other_table.withColumn(
                    'credit_mix_bad',
                    F.when(col('Credit_Mix') == 'BAD', 1).otherwise(0)
                )

                print(f'Created credit_mix_good, credit_mix_standard, credit_mix_bad (one-hot encoding) from Credit_Mix')

            # ============================================================================
            # TEMPORAL CORRECTNESS: Filter customer records by snapshot_date
            # - Only include customer data that existed at or before the loan application
            # - Prevents using future customer information (e.g., attributes from 2025
            #   for a loan applied in 2023)
            # - This ensures point-in-time correctness for features
            # ============================================================================
            if 'snapshot_date' in other_table.columns:
                # Convert snapshot_date to same format for comparison
                other_table = other_table.withColumn(
                    'snapshot_date_converted',
                    F.to_date(col('snapshot_date'))
                )

                # Drop columns that already exist (except Customer_ID which is the join key)
                columns_to_drop = [c for c in other_table.columns
                                 if c in existing_columns and c not in ["Customer_ID", "snapshot_date_converted"]]
                if columns_to_drop:
                    print(f'Dropping duplicate columns from {table_name}: {columns_to_drop}')
                    other_table = other_table.drop(*columns_to_drop)

                # Only use customer records available at or before loan application time
                # Join on Customer_ID with temporal filter
                df_features = df_features.join(
                    other_table,
                    on=[
                        df_features.Customer_ID == other_table.Customer_ID,
                        other_table.snapshot_date_converted <= df_features.snapshot_date
                    ],
                    how="left"
                ).drop(other_table.Customer_ID).drop("snapshot_date_converted")

                print(f'Joined {table_name} features with temporal filter (snapshot_date <= loan application date)')
            else:
                # Fallback: simple join if no snapshot_date column exists
                columns_to_drop = [c for c in other_table.columns
                                 if c in existing_columns and c != "Customer_ID"]
                if columns_to_drop:
                    print(f'Dropping duplicate columns from {table_name}: {columns_to_drop}')
                    other_table = other_table.drop(*columns_to_drop)

                df_features = df_features.join(
                    other_table,
                    on="Customer_ID",
                    how="left"
                )
                print(f'Joined {table_name} features (no temporal filter - snapshot_date column not found)')

    # ============================================================================
    # CLICKSTREAM AGGREGATION: Process clickstream data separately
    # - Clickstream has multiple snapshots per customer over time
    # - Create 6-month rolling average features (avg_fe_1_last_6m, ..., avg_fe_20_last_6m)
    # - Use all snapshots within 6 months before loan application
    # - If fewer than 6 months of data available, compute average based on available data
    #
    # DESIGN CHOICE: 6-month window chosen to align with label observation period (mob=6)
    # In production, this window size should be treated as a hyperparameter and optimized
    # through experimentation (A/B testing or cross-validation)
    # ============================================================================
    if 'clickstream' in available_tables:
        print('Processing clickstream features with 6-month aggregation...')
        clickstream_table = available_tables['clickstream']

        # Convert snapshot_date for temporal filtering
        clickstream_table = clickstream_table.withColumn(
            'snapshot_date_converted',
            F.to_date(col('snapshot_date'))
        )

        # Prepare df_features with loan application date for window calculation
        df_features_with_window = df_features.withColumn(
            'loan_application_date',
            col('snapshot_date')
        ).withColumn(
            'window_start_date',
            F.date_sub(col('loan_application_date'), 180)  # 6 months = ~180 days
        )

        # Join clickstream data with temporal constraint (last 6 months before application)
        # Join condition: Customer_ID matches AND snapshot is within 6-month window
        df_with_clickstream = df_features_with_window.join(
            clickstream_table,
            on=[
                df_features_with_window.Customer_ID == clickstream_table.Customer_ID,
                clickstream_table.snapshot_date_converted >= df_features_with_window.window_start_date,
                clickstream_table.snapshot_date_converted <= df_features_with_window.loan_application_date
            ],
            how="left"
        )

        # Aggregate clickstream features: compute average for each fe_1 to fe_20 over the 6-month window
        # Group by loan_id to get one row per loan application

        # Build aggregation expressions for fe_1 to fe_20
        agg_exprs = []
        for i in range(1, 21):
            fe_col = f"fe_{i}"
            # Average of feature over available snapshots in the window
            agg_exprs.append(F.avg(clickstream_table[fe_col]).alias(f"avg_fe_{i}_last_6m"))

        # Also count how many snapshots were available for transparency
        agg_exprs.append(F.count(clickstream_table.snapshot_date_converted).alias("clickstream_snapshot_count"))

        # Group by all columns from df_features_with_window (before clickstream join)
        group_by_cols = [df_features_with_window[c] for c in df_features_with_window.columns]

        df_features = df_with_clickstream.groupBy(*group_by_cols).agg(*agg_exprs)

        # Drop temporary window calculation columns and clickstream Customer_ID duplicate
        df_features = df_features.drop('loan_application_date', 'window_start_date')

        # Drop clickstream's duplicate Customer_ID if it exists
        try:
            df_features = df_features.drop(clickstream_table.Customer_ID)
        except:
            pass

        print(f'Clickstream features aggregated: 20 features (avg_fe_1_last_6m to avg_fe_20_last_6m) + snapshot_count')

    # Add feature metadata
    df_features = df_features.withColumn("feature_snapshot_date", F.lit(snapshot_date_str).cast(StringType()))

    # Save feature store
    partition_name = f"gold_feature_store_{partition_suffix}.parquet"
    filepath = gold_feature_store_directory + partition_name
    df_features.write.mode("overwrite").parquet(filepath)
    print(f'Feature store saved to: {filepath}, rows: {df_features.count()}')

    return df_features