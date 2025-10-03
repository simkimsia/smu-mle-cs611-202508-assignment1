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

    def clean_numeric_formatting(self, df, column_name):
        """Conservative cleaning for numeric columns - removes underscores, commas, spaces, and dollar signs"""
        df = df.withColumn(
            f"{column_name}_original",
            col(column_name)  # Keep original for audit trail
        )

        # Conservative cleaning: remove common formatting characters
        df = df.withColumn(
            column_name,
            F.regexp_replace(col(column_name).cast(StringType()), "[_,$\\s]", "")
        )

        return df

    def validate_and_handle_categorical(self, df, column_name, acceptable_values, strategy=InvalidDataStrategy.FLAG_ONLY):
        """Validate categorical columns against known acceptable values"""
        # Standardize the column (uppercase, trim)
        df = df.withColumn(
            column_name,
            F.when(col(column_name).isNotNull(), F.upper(F.trim(col(column_name))))
             .otherwise(col(column_name))
        )

        # Convert acceptable values to uppercase for comparison
        acceptable_values_upper = [val.upper() for val in acceptable_values]

        # Create validation flag
        df = df.withColumn(
            f"valid_{column_name.lower()}",
            col(column_name).isNotNull() & col(column_name).isin(acceptable_values_upper)
        )

        return self._handle_invalid_data(df, column_name, f"valid_{column_name.lower()}", strategy)

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
        """Validate amount columns that must be positive (> 0)"""
        df = df.withColumn(
            f"valid_{column_name}",
            (col(column_name).isNotNull()) & (col(column_name) > 0)
        )

        return self._handle_invalid_data(df, column_name, f"valid_{column_name}", strategy, default_value)

    def validate_and_handle_non_negative_amount(self, df, column_name, strategy=InvalidDataStrategy.DEFAULT_VALUE, default_value=0.0):
        """Validate amount columns that can be zero or positive (>= 0)"""
        df = df.withColumn(
            f"valid_{column_name}",
            (col(column_name).isNotNull()) & (col(column_name) >= 0)
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
        # Ensure quarantine goes inside the timestamped datamart directory
        quarantine_path = f"{self.base_datamart_dir}quarantine/{self.__class__.__name__}_{column_name}"

        # Create quarantine directory if it doesn't exist
        os.makedirs(os.path.dirname(quarantine_path), exist_ok=True)

        invalid_df.write.mode("append").parquet(quarantine_path)
        print(f"Quarantined {invalid_df.count()} invalid records to {quarantine_path}")


class BaseSilverProcessor(ABC, ValidationMixin):
    """Base class for silver table processors with common functionality"""

    def __init__(self, spark, base_datamart_dir="datamart/"):
        self.spark = spark
        self.base_datamart_dir = base_datamart_dir

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
        """Apply schema casting to dataframe with numeric cleaning"""
        for column, new_type in column_type_map.items():
            if column in df.columns:
                # Clean numeric columns before casting
                if isinstance(new_type, (FloatType, IntegerType)):
                    df = self.clean_numeric_formatting(df, column)

                # Parse date columns with proper format handling
                if isinstance(new_type, DateType):
                    df = self._parse_date_column(df, column)
                else:
                    df = df.withColumn(column, col(column).cast(new_type))
        return df

    def _parse_date_column(self, df, column_name):
        """Parse date column handling multiple formats (d/M/yy and yyyy-MM-dd)"""
        # Try to parse common date formats
        # Format 1: d/M/yy (e.g., "1/5/23" = May 1, 2023)
        # Format 2: yyyy-MM-dd (e.g., "2023-01-01")
        df = df.withColumn(
            column_name,
            F.coalesce(
                F.to_date(col(column_name), "d/M/yy"),      # Try d/M/yy first
                F.to_date(col(column_name), "yyyy-MM-dd"),  # Then yyyy-MM-dd
                F.to_date(col(column_name))                 # Finally default parsing
            )
        )
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
        """Conservative validation - preserve data with flags"""
        return {
            "Customer_ID": InvalidDataStrategy.DROP_ROW,      # Critical: drop bad IDs
            "snapshot_date": InvalidDataStrategy.DROP_ROW,   # Critical: drop bad dates
            "loan_amt": InvalidDataStrategy.FLAG_ONLY,       # Flag but keep for analysis
            "due_amt": InvalidDataStrategy.FLAG_ONLY,        # Flag but keep
            "paid_amt": InvalidDataStrategy.FLAG_ONLY,       # Flag but keep
            "overdue_amt": InvalidDataStrategy.FLAG_ONLY,    # Flag but keep
            "balance": InvalidDataStrategy.FLAG_ONLY,        # Flag but keep
            "loan_start_date": InvalidDataStrategy.FLAG_ONLY, # Flag but keep
        }

    def apply_business_logic(self, df):
        """Apply loan-specific business logic with additional validations"""
        # Validate financial amounts before business logic
        config = self.get_validation_config()

        # loan_amt must be positive (> 0) - you can't have a $0 loan
        if "loan_amt" in df.columns:
            strategy = config.get("loan_amt", InvalidDataStrategy.QUARANTINE)
            df = self.validate_and_handle_positive_amount(df, "loan_amt", strategy)

        # These amounts can be zero (>= 0) - valid business cases
        for amount_col in ["due_amt", "paid_amt", "overdue_amt", "balance"]:
            if amount_col in df.columns:
                strategy = config.get(amount_col, InvalidDataStrategy.DEFAULT_VALUE)
                default_val = 0.0 if strategy == InvalidDataStrategy.DEFAULT_VALUE else None
                df = self.validate_and_handle_non_negative_amount(df, amount_col, strategy, default_val)

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
        # Keep in silver for data lineage, exclude from gold for ML
        if "SSN" in df.columns:
            df = df.withColumn(
                "valid_ssn",
                col("SSN").rlike("^\\d{3}-\\d{2}-\\d{4}$")
            )
            # Just flag, don't modify SSN data

        # Name kept in silver for data lineage
        # Will be excluded from gold layer (no predictive value)

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
        """Conservative validation - flag most issues, quarantine only extreme outliers"""
        return {
            "Customer_ID": InvalidDataStrategy.DROP_ROW,           # Critical: drop bad IDs
            "snapshot_date": InvalidDataStrategy.DROP_ROW,        # Critical: drop bad dates
            "Annual_Income": InvalidDataStrategy.FLAG_ONLY,       # Flag but keep most cases
            "Monthly_Inhand_Salary": InvalidDataStrategy.FLAG_ONLY, # Flag but keep most cases
            "Interest_Rate": InvalidDataStrategy.FLAG_ONLY,       # Flag but keep
            "Credit_Utilization_Ratio": InvalidDataStrategy.FLAG_ONLY, # Flag but keep
            "Outstanding_Debt": InvalidDataStrategy.FLAG_ONLY,    # Flag but keep
            "Monthly_Balance": InvalidDataStrategy.FLAG_ONLY,     # Flag but keep
            "Credit_Mix": InvalidDataStrategy.FLAG_ONLY,          # Flag but keep "_" values
            "Payment_Behaviour": InvalidDataStrategy.FLAG_ONLY,   # Flag invalid categorical values
            "Payment_of_Min_Amount": InvalidDataStrategy.FLAG_ONLY, # Flag invalid categorical values
            "Type_of_Loan": InvalidDataStrategy.FLAG_ONLY,        # Flag invalid categorical values
        }

    def get_categorical_validation_rules(self):
        """Define acceptable values for categorical fields"""
        return {
            "Payment_Behaviour": [
                "High_spent_Medium_value_payments",
                "High_spent_Small_value_payments",
                "Low_spent_Medium_value_payments",
                "Low_spent_Small_value_payments",
                "High_spent_Large_value_payments",
                "Low_spent_Large_value_payments"
            ],
            "Payment_of_Min_Amount": [
                "Yes", "No"  # Valid values - NM will be flagged as invalid
            ],
            "Credit_Mix": [
                "Bad", "Good", "Standard"
            ]
        }

    def apply_business_logic(self, df):
        """Apply financial-specific business logic with comprehensive validation"""
        config = self.get_validation_config()

        # Handle extreme outliers for income (quarantine) vs normal validation (flag)
        if "Annual_Income" in df.columns:
            # First, quarantine extreme outliers (< $1K or > $10M)
            extreme_outliers = df.filter(
                col("Annual_Income").isNotNull() &
                ((col("Annual_Income") < 1000) | (col("Annual_Income") > 10000000))
            )
            if extreme_outliers.count() > 0:
                self._quarantine_invalid_data(extreme_outliers, "Annual_Income")
                df = df.filter(
                    col("Annual_Income").isNull() |
                    ((col("Annual_Income") >= 1000) & (col("Annual_Income") <= 10000000))
                )

            # Then flag remaining validation issues
            df = df.withColumn(
                "valid_annual_income",
                col("Annual_Income").isNotNull() &
                (col("Annual_Income") >= 10000) &
                (col("Annual_Income") <= 10000000)
            )
            df = self._handle_invalid_data(df, "Annual_Income", "valid_annual_income",
                                         config.get("Annual_Income"))

        if "Monthly_Inhand_Salary" in df.columns:
            # First, quarantine extreme outliers (< $100 or > $1M)
            extreme_outliers = df.filter(
                col("Monthly_Inhand_Salary").isNotNull() &
                ((col("Monthly_Inhand_Salary") < 100) | (col("Monthly_Inhand_Salary") > 1000000))
            )
            if extreme_outliers.count() > 0:
                self._quarantine_invalid_data(extreme_outliers, "Monthly_Inhand_Salary")
                df = df.filter(
                    col("Monthly_Inhand_Salary").isNull() |
                    ((col("Monthly_Inhand_Salary") >= 100) & (col("Monthly_Inhand_Salary") <= 1000000))
                )

            # Then flag remaining validation issues
            df = df.withColumn(
                "valid_monthly_salary",
                col("Monthly_Inhand_Salary").isNotNull() &
                (col("Monthly_Inhand_Salary") >= 1000) &
                (col("Monthly_Inhand_Salary") <= 100000)
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

        # Validate non-negative amounts with defaults (these can be zero)
        for amount_col in ["Outstanding_Debt", "Monthly_Balance", "Total_EMI_per_month", "Amount_invested_monthly"]:
            if amount_col in df.columns:
                strategy = config.get(amount_col, InvalidDataStrategy.DEFAULT_VALUE)
                default_val = 0.0 if strategy == InvalidDataStrategy.DEFAULT_VALUE else None
                df = self.validate_and_handle_non_negative_amount(df, amount_col, strategy, default_val)

        # Validate categorical fields with known acceptable values
        categorical_rules = self.get_categorical_validation_rules()

        for column_name, acceptable_values in categorical_rules.items():
            if column_name in df.columns:
                # Special handling for Credit_Mix: convert "_" to null first
                if column_name == "Credit_Mix":
                    df = df.withColumn(
                        column_name,
                        F.when(col(column_name) == "_", F.lit(None))
                         .otherwise(col(column_name))
                    )

                # Apply categorical validation
                strategy = config.get(column_name, InvalidDataStrategy.FLAG_ONLY)
                df = self.validate_and_handle_categorical(df, column_name, acceptable_values, strategy)

        # Handle Type_of_Loan - light touch approach for messy multi-value field
        if "Type_of_Loan" in df.columns:
            df = df.withColumn(
                "Type_of_Loan",
                F.when(col("Type_of_Loan") == "", F.lit(None))  # Empty string → null
                 .when(col("Type_of_Loan").isNotNull(), F.trim(col("Type_of_Loan")))  # Just trim
                 .otherwise(col("Type_of_Loan"))
            )

            # Add completeness and usefulness flag
            df = df.withColumn(
                "valid_type_of_loan",
                col("Type_of_Loan").isNotNull() &
                (col("Type_of_Loan") != "Not Specified") &
                (F.length(col("Type_of_Loan")) > 0)
            )

            # Apply FLAG_ONLY strategy for data quality tracking
            strategy = config.get("Type_of_Loan", InvalidDataStrategy.FLAG_ONLY)
            df = self._handle_invalid_data(df, "Type_of_Loan", "valid_type_of_loan", strategy)

            # ============================================================================
            # FEATURE ENGINEERING: Type_of_Loan Multi-Label Binarization
            # - Extract unique loan types from comma-separated values
            # - Create binary indicator columns (has_payday_loan, has_mortgage_loan, etc.)
            # - Ignore duplicates (e.g., "Payday Loan, Payday Loan" → has_payday_loan=1)
            # - Exclude "Not Specified" as it's metadata, not a real loan type
            # - Separators handled: ", " and " and "
            # ============================================================================

            # Extract all unique loan types from the data
            unique_loan_types_df = df.select("Type_of_Loan") \
                .filter(col("Type_of_Loan").isNotNull()) \
                .distinct() \
                .collect()

            # Parse all unique loan types
            all_loan_types_set = set()
            for row in unique_loan_types_df:
                if row["Type_of_Loan"]:
                    value = row["Type_of_Loan"]
                    # Split by both ", " and " and "
                    loans = value.replace(' and ', ',').split(',')
                    # Clean whitespace and filter out "Not Specified"
                    loans = [loan.strip() for loan in loans
                            if loan.strip() and loan.strip() != 'Not Specified' and 'Loan' in loan.strip()]
                    all_loan_types_set.update(loans)

            unique_loan_types = sorted(all_loan_types_set)
            print(f"Found {len(unique_loan_types)} unique loan types: {unique_loan_types}")

            # Create binary columns for each loan type
            for loan_type in unique_loan_types:
                feature_name = f"has_{loan_type.lower().replace(' ', '_').replace('-', '_')}"
                # Use regexp to check if loan type exists (handles separators automatically)
                # Wrap in word boundaries to avoid partial matches
                df = df.withColumn(
                    feature_name,
                    F.when(
                        col("Type_of_Loan").isNotNull() &
                        col("Type_of_Loan").rlike(f"\\b{loan_type}\\b"),
                        1
                    ).otherwise(0)
                )

            # Add derived features for loan type analysis
            # has_any_loan: 1 if user has any valid loan type, 0 otherwise
            df = df.withColumn(
                "has_any_loan",
                F.when(
                    col("Type_of_Loan").isNotNull() &
                    (col("Type_of_Loan") != "Not Specified") &
                    (F.length(col("Type_of_Loan")) > 0),
                    1
                ).otherwise(0)
            )

            print(f"Created {len(unique_loan_types)} binary loan type features + has_any_loan")

        # Parse Credit_History_Age from string format to integer months
        # Example: "10 Years and 9 Months" → 129 months
        if "Credit_History_Age" in df.columns:
            # Extract years and months using regex
            # Pattern: "(\d+) Years and (\d+) Months" or "(\d+) Year and (\d+) Month" (singular)
            # Also handle edge cases: "10 Years", "5 Months", etc.

            # Extract years (defaults to 0 if not present)
            df = df.withColumn(
                "years_part",
                F.regexp_extract(col("Credit_History_Age"), r"(\d+)\s+Years?", 1).cast(IntegerType())
            )

            # Extract months (defaults to 0 if not present)
            df = df.withColumn(
                "months_part",
                F.regexp_extract(col("Credit_History_Age"), r"(\d+)\s+Months?", 1).cast(IntegerType())
            )

            # Calculate total months: (years * 12) + months
            # Handle nulls: if both parts are null/0, result is null (invalid data)
            df = df.withColumn(
                "Credit_History_Age_Months",
                F.when(
                    col("Credit_History_Age").isNotNull(),
                    F.coalesce(col("years_part"), F.lit(0)) * 12 + F.coalesce(col("months_part"), F.lit(0))
                ).otherwise(F.lit(None)).cast(IntegerType())
            )

            # Add validation flag
            df = df.withColumn(
                "valid_credit_history_age",
                col("Credit_History_Age_Months").isNotNull() &
                (col("Credit_History_Age_Months") >= 0) &
                (col("Credit_History_Age_Months") <= 600)  # Max 50 years seems reasonable
            )

            # Drop temporary columns
            df = df.drop("years_part", "months_part")

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
    def get_processor(cls, table_type, spark, base_datamart_dir="datamart/"):
        """Get processor instance for the given table type"""
        processor_class = cls._processors.get(table_type)
        if not processor_class:
            raise ValueError(f"No processor found for table type: {table_type}")
        return processor_class(spark, base_datamart_dir)

    @classmethod
    def get_available_processors(cls):
        """Get list of available processor types"""
        return list(cls._processors.keys())


def process_silver_table(table_type, bronze_filepath, silver_filepath, spark, base_datamart_dir="datamart/"):
    """
    Process a bronze table to silver using the appropriate processor

    Args:
        table_type: Type of table ('loan_daily', 'clickstream', 'attributes', 'financials')
        bronze_filepath: Path to bronze table file
        silver_filepath: Path to save silver table
        spark: Spark session
        base_datamart_dir: Base datamart directory for quarantine files
    """
    processor = SilverProcessorFactory.get_processor(table_type, spark, base_datamart_dir)
    return processor.process(bronze_filepath, silver_filepath)


# Backwards compatibility function
def process_silver_table_legacy(snapshot_date_str, bronze_lms_directory, silver_loan_daily_directory, spark, base_datamart_dir="datamart/"):
    """Legacy function for backwards compatibility - processes loan daily data only"""
    # Build file paths
    bronze_filename = f"bronze_lms_loan_daily_{snapshot_date_str.replace('-','_')}.csv"
    bronze_filepath = os.path.join(bronze_lms_directory, bronze_filename)

    silver_filename = f"silver_loan_daily_{snapshot_date_str.replace('-','_')}.parquet"
    silver_filepath = os.path.join(silver_loan_daily_directory, silver_filename)

    # Process using new modular approach
    return process_silver_table('loan_daily', bronze_filepath, silver_filepath, spark, base_datamart_dir)