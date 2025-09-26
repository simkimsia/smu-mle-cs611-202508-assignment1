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
from abc import ABC, abstractmethod


class BaseSilverProcessor(ABC):
    """Base class for silver table processors with common functionality"""

    def __init__(self, spark):
        self.spark = spark

    def load_bronze_data(self, bronze_filepath):
        """Load data from bronze layer"""
        df = self.spark.read.csv(bronze_filepath, header=True, inferSchema=True)
        print(f'loaded from: {bronze_filepath}, row count: {df.count()}')
        return df

    def save_silver_data(self, df, silver_filepath):
        """Save data to silver layer"""
        df.write.mode("overwrite").parquet(silver_filepath)
        print(f'saved to: {silver_filepath}')
        return df

    def apply_schema(self, df, column_type_map):
        """Apply schema casting to dataframe"""
        for column, new_type in column_type_map.items():
            if column in df.columns:
                df = df.withColumn(column, col(column).cast(new_type))
        return df

    @abstractmethod
    def get_column_type_map(self):
        """Return column type mapping for this table"""
        pass

    @abstractmethod
    def apply_business_logic(self, df):
        """Apply table-specific business logic and data augmentation"""
        pass

    def process(self, bronze_filepath, silver_filepath):
        """Main processing pipeline"""
        # Load data
        df = self.load_bronze_data(bronze_filepath)

        # Apply schema
        column_type_map = self.get_column_type_map()
        df = self.apply_schema(df, column_type_map)

        # Apply business logic
        df = self.apply_business_logic(df)

        # Save data
        return self.save_silver_data(df, silver_filepath)


class LoanDailySilverProcessor(BaseSilverProcessor):
    """Silver processor for loan daily data"""

    def get_column_type_map(self):
        return {
            "loan_id": StringType(),
            "Customer_ID": StringType(),
            "loan_start_date": DateType(),
            "tenure": IntegerType(),
            "installment_num": IntegerType(),
            "loan_amt": FloatType(),
            "due_amt": FloatType(),
            "paid_amt": FloatType(),
            "overdue_amt": FloatType(),
            "balance": FloatType(),
            "snapshot_date": DateType(),
        }

    def apply_business_logic(self, df):
        """Apply loan-specific business logic"""
        # Add month on book
        df = df.withColumn("mob", col("installment_num").cast(IntegerType()))

        # Add days past due calculations
        df = df.withColumn("installments_missed",
                          F.ceil(col("overdue_amt") / col("due_amt")).cast(IntegerType())).fillna(0)

        df = df.withColumn("first_missed_date",
                          F.when(col("installments_missed") > 0,
                                F.add_months(col("snapshot_date"), -1 * col("installments_missed"))).cast(DateType()))

        df = df.withColumn("dpd",
                          F.when(col("overdue_amt") > 0.0,
                                F.datediff(col("snapshot_date"), col("first_missed_date"))).otherwise(0).cast(IntegerType()))

        return df


class ClickstreamSilverProcessor(BaseSilverProcessor):
    """Silver processor for clickstream data"""

    def get_column_type_map(self):
        column_map = {
            "Customer_ID": StringType(),
            "snapshot_date": DateType(),
        }
        # Add fe_1 to fe_20 as IntegerType columns
        for i in range(1, 21):
            column_map[f"fe_{i}"] = IntegerType()

        return column_map

    def apply_business_logic(self, df):
        """Apply clickstream-specific business logic"""
        # Example: session aggregations, event categorization, etc.
        return df


class AttributesSilverProcessor(BaseSilverProcessor):
    """Silver processor for user attributes data"""

    def get_column_type_map(self):
        return {
            "Customer_ID": StringType(),
            "Name": StringType(),
            "Age": IntegerType(),
            "SSN": StringType(),
            "Occupation": StringType(),
            "snapshot_date": DateType(),
        }

    def apply_business_logic(self, df):
        """Apply attributes-specific business logic"""
        # Example: standardize categorical values, handle missing data
        return df


class FinancialsSilverProcessor(BaseSilverProcessor):
    """Silver processor for financial data"""

    def get_column_type_map(self):
        return {
            "Customer_ID": StringType(),
            "Annual_Income": FloatType(),
            "Monthly_Inhand_Salary": FloatType(),
            "Num_Bank_Accounts": IntegerType(),
            "Num_Credit_Card": IntegerType(),
            "Interest_Rate": FloatType(),
            "Num_of_Loan": IntegerType(),
            "Type_of_Loan": StringType(),
            "Delay_from_due_date": IntegerType(),
            "Num_of_Delayed_Payment": IntegerType(),
            "Changed_Credit_Limit": FloatType(),
            "Num_Credit_Inquiries": FloatType(),
            "Credit_Mix": StringType(),
            "Outstanding_Debt": FloatType(),
            "Credit_Utilization_Ratio": FloatType(),
            "Credit_History_Age": StringType(),
            "Payment_of_Min_Amount": StringType(),
            "Total_EMI_per_month": FloatType(),
            "Amount_invested_monthly": FloatType(),
            "Payment_Behaviour": StringType(),
            "Monthly_Balance": FloatType(),
            "snapshot_date": DateType(),
        }

    def apply_business_logic(self, df):
        """Apply financial-specific business logic"""
        # Example: calculate derived financial metrics, handle outliers
        return df


class SilverProcessorFactory:
    """Factory to get the appropriate silver processor for a table"""

    _processors = {
        'loan_daily': LoanDailySilverProcessor,
        'clickstream': ClickstreamSilverProcessor,
        'attributes': AttributesSilverProcessor,
        'financials': FinancialsSilverProcessor,
    }

    @classmethod
    def get_processor(cls, table_type, spark):
        """Get processor instance for the given table type"""
        processor_class = cls._processors.get(table_type)
        if not processor_class:
            raise ValueError(f"No processor found for table type: {table_type}")
        return processor_class(spark)

    @classmethod
    def get_available_processors(cls):
        """Get list of available processor types"""
        return list(cls._processors.keys())


def process_silver_table(table_type, bronze_filepath, silver_filepath, spark):
    """
    Process a bronze table to silver using the appropriate processor

    Args:
        table_type: Type of table ('loan_daily', 'clickstream', 'attributes', 'financials')
        bronze_filepath: Path to bronze table file
        silver_filepath: Path to save silver table
        spark: Spark session
    """
    processor = SilverProcessorFactory.get_processor(table_type, spark)
    return processor.process(bronze_filepath, silver_filepath)


# Backwards compatibility function
def process_silver_table_legacy(snapshot_date_str, bronze_lms_directory, silver_loan_daily_directory, spark):
    """Legacy function for backwards compatibility - processes loan daily data only"""
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d")

    # Build file paths
    bronze_filename = f"bronze_loan_daily_{snapshot_date_str.replace('-','_')}.csv"
    bronze_filepath = os.path.join(bronze_lms_directory, bronze_filename)

    silver_filename = f"silver_loan_daily_{snapshot_date_str.replace('-','_')}.parquet"
    silver_filepath = os.path.join(silver_loan_daily_directory, silver_filename)

    # Process using new modular approach
    return process_silver_table('loan_daily', bronze_filepath, silver_filepath, spark)