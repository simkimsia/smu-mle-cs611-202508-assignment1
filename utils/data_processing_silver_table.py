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
from enum import Enum

from pyspark.sql.functions import col
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType
from abc import ABC, abstractmethod


class InvalidDataStrategy(Enum):
    """Strategies for handling invalid data"""
    DROP_ROW = "drop_row"           # Remove entire record
    FLAG_ONLY = "flag_only"         # Keep record, add validation flag
    DEFAULT_VALUE = "default_value"  # Replace with default
    NULL_VALUE = "null_value"       # Set to null
    QUARANTINE = "quarantine"       # Move to separate table


class ValidationMixin:
    """Mixin class providing common validation and data handling methods"""

    def validate_and_handle_customer_id(self, df, strategy=InvalidDataStrategy.FLAG_ONLY, default_value=None):
        """Validate Customer_ID and handle invalid values"""
        df = df.withColumn(
            "valid_customer_id",
            col("Customer_ID").rlike("^CUS_0x[0-9A-Fa-f]+$")
        )

        return self._handle_invalid_data(df, "Customer_ID", "valid_customer_id", strategy, default_value)

    def validate_and_handle_date_column(self, df, column_name, strategy=InvalidDataStrategy.NULL_VALUE):
        """Validate date columns with configurable handling"""
        df = df.withColumn(
            f"valid_{column_name}",
            col(column_name).isNotNull() &
            (col(column_name) >= F.lit("2020-01-01")) &
            (col(column_name) <= F.current_date())
        )

        return self._handle_invalid_data(df, column_name, f"valid_{column_name}", strategy)

    def validate_and_handle_positive_amount(self, df, column_name, strategy=InvalidDataStrategy.DEFAULT_VALUE, default_value=0.0):
        """Validate amount columns with configurable handling"""
        df = df.withColumn(
            f"valid_{column_name}",
            (col(column_name).isNotNull()) & (col(column_name) > 0)
        )

        return self._handle_invalid_data(df, column_name, f"valid_{column_name}", strategy, default_value)

    def _handle_invalid_data(self, df, data_column, validation_flag_column, strategy, default_value=None):
        """Handle invalid data based on strategy"""
        if strategy == InvalidDataStrategy.DROP_ROW:
            # Remove rows where validation failed
            df = df.filter(col(validation_flag_column) == True)

        elif strategy == InvalidDataStrategy.FLAG_ONLY:
            # Keep all data, just maintain validation flags
            pass

        elif strategy == InvalidDataStrategy.DEFAULT_VALUE:
            # Replace invalid values with default
            if default_value is not None:
                df = df.withColumn(
                    data_column,
                    F.when(col(validation_flag_column), col(data_column))
                     .otherwise(F.lit(default_value))
                )

        elif strategy == InvalidDataStrategy.NULL_VALUE:
            # Set invalid values to null
            df = df.withColumn(
                data_column,
                F.when(col(validation_flag_column), col(data_column))
                 .otherwise(F.lit(None))
            )

        elif strategy == InvalidDataStrategy.QUARANTINE:
            # This would require additional logic to save invalid records separately
            invalid_df = df.filter(col(validation_flag_column) == False)
            self._quarantine_invalid_data(invalid_df, data_column)
            df = df.filter(col(validation_flag_column) == True)

        return df

    def _quarantine_invalid_data(self, invalid_df, column_name):
        """Save invalid data to quarantine location"""
        quarantine_path = f"datamart/quarantine/{self.__class__.__name__}_{column_name}"
        invalid_df.write.mode("append").parquet(quarantine_path)
        print(f"Quarantined {invalid_df.count()} invalid records to {quarantine_path}")


class BaseSilverProcessor(ABC, ValidationMixin):
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

    def get_validation_config(self):
        """Override in subclasses for processor-specific validation rules"""
        return {
            "Customer_ID": InvalidDataStrategy.FLAG_ONLY,
            "snapshot_date": InvalidDataStrategy.NULL_VALUE,
        }

    def apply_validations_and_handling(self, df):
        """Apply validations with processor-specific handling"""
        config = self.get_validation_config()

        # Apply Customer_ID validation if present
        if "Customer_ID" in df.columns:
            strategy = config.get("Customer_ID", InvalidDataStrategy.FLAG_ONLY)
            df = self.validate_and_handle_customer_id(df, strategy)

        # Apply date validation
        if "snapshot_date" in df.columns:
            strategy = config.get("snapshot_date", InvalidDataStrategy.NULL_VALUE)
            df = self.validate_and_handle_date_column(df, "snapshot_date", strategy)

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

        # Apply validations
        df = self.apply_validations_and_handling(df)

        # Apply business logic
        df = self.apply_business_logic(df)

        # Save data
        return self.save_silver_data(df, silver_filepath)


class LoanDailySilverProcessor(BaseSilverProcessor):
    """Silver processor for loan daily data - Critical financial data with strict validation"""

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

    def get_validation_config(self):
        """Strict validation for critical financial data"""
        return {
            "Customer_ID": InvalidDataStrategy.DROP_ROW,      # Critical: drop bad IDs
            "loan_amt": InvalidDataStrategy.QUARANTINE,      # Critical: quarantine bad amounts
            "due_amt": InvalidDataStrategy.DEFAULT_VALUE,    # Business logic: default to 0
            "paid_amt": InvalidDataStrategy.DEFAULT_VALUE,   # Business logic: default to 0
            "overdue_amt": InvalidDataStrategy.DEFAULT_VALUE, # Business logic: default to 0
            "balance": InvalidDataStrategy.DEFAULT_VALUE,    # Business logic: default to 0
            "snapshot_date": InvalidDataStrategy.DROP_ROW,   # Critical: drop bad dates
            "loan_start_date": InvalidDataStrategy.NULL_VALUE, # Allow null for missing start dates
        }

    def apply_business_logic(self, df):
        """Apply loan-specific business logic with additional validations"""
        # Validate financial amounts before business logic
        config = self.get_validation_config()

        for amount_col in ["loan_amt", "due_amt", "paid_amt", "overdue_amt", "balance"]:
            if amount_col in df.columns:
                strategy = config.get(amount_col, InvalidDataStrategy.DEFAULT_VALUE)
                default_val = 0.0 if strategy == InvalidDataStrategy.DEFAULT_VALUE else None
                df = self.validate_and_handle_positive_amount(df, amount_col, strategy, default_val)

        # Add month on book
        df = df.withColumn("mob", col("installment_num").cast(IntegerType()))

        # Add days past due calculations with null handling
        df = df.withColumn("installments_missed",
                          F.when((col("overdue_amt").isNotNull()) & (col("due_amt").isNotNull()) & (col("due_amt") > 0),
                                F.ceil(col("overdue_amt") / col("due_amt"))).otherwise(0).cast(IntegerType()))

        df = df.withColumn("first_missed_date",
                          F.when(col("installments_missed") > 0,
                                F.add_months(col("snapshot_date"), -1 * col("installments_missed"))).cast(DateType()))

        df = df.withColumn("dpd",
                          F.when((col("overdue_amt").isNotNull()) & (col("overdue_amt") > 0.0) & (col("first_missed_date").isNotNull()),
                                F.datediff(col("snapshot_date"), col("first_missed_date"))).otherwise(0).cast(IntegerType()))

        return df


class ClickstreamSilverProcessor(BaseSilverProcessor):
    """Silver processor for clickstream data - Non-critical behavioral data with lenient validation"""

    def get_column_type_map(self):
        column_map = {
            "Customer_ID": StringType(),
            "snapshot_date": DateType(),
        }
        # Add fe_1 to fe_20 as IntegerType columns
        for i in range(1, 21):
            column_map[f"fe_{i}"] = IntegerType()

        return column_map

    def get_validation_config(self):
        """Lenient validation for behavioral data"""
        config = {
            "Customer_ID": InvalidDataStrategy.FLAG_ONLY,    # Keep for analysis
            "snapshot_date": InvalidDataStrategy.NULL_VALUE, # Nullify bad dates
        }
        # fe_1 to fe_20 can have default values
        for i in range(1, 21):
            config[f"fe_{i}"] = InvalidDataStrategy.DEFAULT_VALUE

        return config

    def apply_business_logic(self, df):
        """Apply clickstream-specific business logic with feature validation"""
        config = self.get_validation_config()

        # Validate feature columns (fe_1 to fe_20) with default value 0
        for i in range(1, 21):
            fe_col = f"fe_{i}"
            if fe_col in df.columns:
                strategy = config.get(fe_col, InvalidDataStrategy.DEFAULT_VALUE)
                df = df.withColumn(
                    f"valid_{fe_col}",
                    col(fe_col).isNotNull() & (col(fe_col) >= 0)
                )
                df = self._handle_invalid_data(df, fe_col, f"valid_{fe_col}", strategy, 0)

        return df


class AttributesSilverProcessor(BaseSilverProcessor):
    """Silver processor for user attributes data - Reference data with moderate validation"""

    def get_column_type_map(self):
        return {
            "Customer_ID": StringType(),
            "Name": StringType(),
            "Age": IntegerType(),
            "SSN": StringType(),
            "Occupation": StringType(),
            "snapshot_date": DateType(),
        }

    def get_validation_config(self):
        """Moderate validation for reference data"""
        return {
            "Customer_ID": InvalidDataStrategy.FLAG_ONLY,    # Keep for joining
            "Age": InvalidDataStrategy.FLAG_ONLY,            # Flag invalid but keep for ML
            "SSN": InvalidDataStrategy.FLAG_ONLY,            # Flag invalid but keep
            "snapshot_date": InvalidDataStrategy.NULL_VALUE, # Nullify bad dates
        }

    def apply_business_logic(self, df):
        """Apply attributes-specific business logic with validation"""
        # Validate age range (18-100 years) but keep original values for ML
        if "Age" in df.columns:
            df = df.withColumn(
                "valid_age",
                col("Age").isNotNull() & (col("Age") >= 18) & (col("Age") <= 100)
            )
            # Just flag, don't modify age values - let ML handle edge cases

            # Optional: Cap extreme outliers while preserving most data
            df = df.withColumn(
                "Age_capped",
                F.when(col("Age") < 0, 18)  # Negative ages become minimum
                 .when(col("Age") > 150, 65)  # Extreme ages become reasonable senior
                 .otherwise(col("Age"))
            )

        # Validate SSN format (assuming XXX-XX-XXXX format)
        if "SSN" in df.columns:
            df = df.withColumn(
                "valid_ssn",
                col("SSN").rlike("^\\d{3}-\\d{2}-\\d{4}$")
            )
            # Just flag, don't modify SSN data

        # Standardize occupation values
        if "Occupation" in df.columns:
            df = df.withColumn(
                "Occupation",
                F.when(col("Occupation").isNotNull(), F.upper(F.trim(col("Occupation"))))
                 .otherwise(col("Occupation"))
            )

        return df


class FinancialsSilverProcessor(BaseSilverProcessor):
    """Silver processor for financial data - Credit data with strict validation"""

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

    def get_validation_config(self):
        """Strict validation for credit data"""
        return {
            "Customer_ID": InvalidDataStrategy.DROP_ROW,           # Critical: drop bad IDs
            "Annual_Income": InvalidDataStrategy.QUARANTINE,      # Critical: quarantine unrealistic incomes
            "Monthly_Inhand_Salary": InvalidDataStrategy.QUARANTINE, # Critical: quarantine unrealistic salaries
            "Interest_Rate": InvalidDataStrategy.NULL_VALUE,      # Nullify invalid rates
            "Credit_Utilization_Ratio": InvalidDataStrategy.NULL_VALUE, # Nullify invalid ratios
            "Outstanding_Debt": InvalidDataStrategy.DEFAULT_VALUE, # Default to 0
            "Monthly_Balance": InvalidDataStrategy.DEFAULT_VALUE,  # Default to 0
            "snapshot_date": InvalidDataStrategy.DROP_ROW,        # Critical: drop bad dates
        }

    def apply_business_logic(self, df):
        """Apply financial-specific business logic with comprehensive validation"""
        config = self.get_validation_config()

        # Validate income ranges (Annual: $10K-$10M, Monthly: $1K-$1M)
        if "Annual_Income" in df.columns:
            df = df.withColumn(
                "valid_annual_income",
                col("Annual_Income").isNotNull() &
                (col("Annual_Income") >= 10000) &
                (col("Annual_Income") <= 10000000)
            )
            df = self._handle_invalid_data(df, "Annual_Income", "valid_annual_income",
                                         config.get("Annual_Income"))

        if "Monthly_Inhand_Salary" in df.columns:
            df = df.withColumn(
                "valid_monthly_salary",
                col("Monthly_Inhand_Salary").isNotNull() &
                (col("Monthly_Inhand_Salary") >= 1000) &
                (col("Monthly_Inhand_Salary") <= 1000000)
            )
            df = self._handle_invalid_data(df, "Monthly_Inhand_Salary", "valid_monthly_salary",
                                         config.get("Monthly_Inhand_Salary"))

        # Validate credit utilization ratio (0-1 range)
        if "Credit_Utilization_Ratio" in df.columns:
            df = df.withColumn(
                "valid_credit_utilization",
                col("Credit_Utilization_Ratio").isNotNull() &
                (col("Credit_Utilization_Ratio") >= 0) &
                (col("Credit_Utilization_Ratio") <= 1)
            )
            df = self._handle_invalid_data(df, "Credit_Utilization_Ratio", "valid_credit_utilization",
                                         config.get("Credit_Utilization_Ratio"))

        # Validate interest rate (0-100% range)
        if "Interest_Rate" in df.columns:
            df = df.withColumn(
                "valid_interest_rate",
                col("Interest_Rate").isNotNull() &
                (col("Interest_Rate") >= 0) &
                (col("Interest_Rate") <= 100)
            )
            df = self._handle_invalid_data(df, "Interest_Rate", "valid_interest_rate",
                                         config.get("Interest_Rate"))

        # Validate positive amounts with defaults
        for amount_col in ["Outstanding_Debt", "Monthly_Balance", "Total_EMI_per_month", "Amount_invested_monthly"]:
            if amount_col in df.columns:
                strategy = config.get(amount_col, InvalidDataStrategy.DEFAULT_VALUE)
                default_val = 0.0 if strategy == InvalidDataStrategy.DEFAULT_VALUE else None
                df = self.validate_and_handle_positive_amount(df, amount_col, strategy, default_val)

        # Standardize categorical fields
        categorical_cols = ["Credit_Mix", "Payment_of_Min_Amount", "Payment_Behaviour", "Type_of_Loan"]
        for cat_col in categorical_cols:
            if cat_col in df.columns:
                df = df.withColumn(
                    cat_col,
                    F.when(col(cat_col).isNotNull(), F.upper(F.trim(col(cat_col))))
                     .otherwise(col(cat_col))
                )

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
    # Build file paths
    bronze_filename = f"bronze_loan_daily_{snapshot_date_str.replace('-','_')}.csv"
    bronze_filepath = os.path.join(bronze_lms_directory, bronze_filename)

    silver_filename = f"silver_loan_daily_{snapshot_date_str.replace('-','_')}.parquet"
    silver_filepath = os.path.join(silver_loan_daily_directory, silver_filename)

    # Process using new modular approach
    return process_silver_table('loan_daily', bronze_filepath, silver_filepath, spark)