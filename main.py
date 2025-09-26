import glob
import os
from datetime import datetime

import pyspark

import utils.data_processing_bronze_table
import utils.data_processing_gold_table
import utils.data_processing_silver_table

# Initialize SparkSession
spark = pyspark.sql.SparkSession.builder.appName("dev").master("local[*]").getOrCreate()

# Set log level to ERROR to hide warnings
spark.sparkContext.setLogLevel("ERROR")

# set up config
snapshot_date_str = "2023-01-01"

start_date_str = "2023-01-01"
end_date_str = "2024-12-01"


# generate list of dates to process
def generate_first_of_month_dates(start_date_str, end_date_str):
    # Convert the date strings to datetime objects
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    # List to store the first of month dates
    first_of_month_dates = []

    # Start from the first of the month of the start_date
    current_date = datetime(start_date.year, start_date.month, 1)

    while current_date <= end_date:
        # Append the date in yyyy-mm-dd format
        first_of_month_dates.append(current_date.strftime("%Y-%m-%d"))

        # Move to the first of the next month
        if current_date.month == 12:
            current_date = datetime(current_date.year + 1, 1, 1)
        else:
            current_date = datetime(current_date.year, current_date.month + 1, 1)

    return first_of_month_dates


dates_str_lst = generate_first_of_month_dates(start_date_str, end_date_str)
print(dates_str_lst)

# create bronze datalake
bronze_directory = "datamart/bronze/"

if not os.path.exists(bronze_directory):
    os.makedirs(bronze_directory)

# discover all CSV files in data folder
data_folder = "data"
csv_files = glob.glob(os.path.join(data_folder, "*.csv"))
print(
    f"Found CSV files for bronze processing: {[os.path.basename(f) for f in csv_files]}"
)

# define output directory mapping for different data sources
output_directory_mapping = {
    "lms_loan_daily.csv": "datamart/bronze/lms/",
    "features_attributes.csv": "datamart/bronze/features/attributes",
    "features_financials.csv": "datamart/bronze/features/financials",
    "feature_clickstream.csv": "datamart/bronze/features/clickstream",
}

# run bronze backfill for all CSV files
for csv_file in csv_files:
    filename = os.path.basename(csv_file)
    file_prefix = os.path.splitext(filename)[0]
    target_directory = output_directory_mapping.get(filename, bronze_directory)

    print(f"\nProcessing bronze backfill for: {filename}")
    print(f"  → Output directory: {target_directory}")

    # check if file has snapshot_date column (only check once, not for each date)
    sample_df = spark.read.csv(csv_file, header=True, inferSchema=True)
    has_snapshot_date = "snapshot_date" in sample_df.columns

    if has_snapshot_date:
        print("  ✓ Has snapshot_date column - processing with date filter")
        # process for each date in the backfill
        for date_str in dates_str_lst:
            utils.data_processing_bronze_table.process_bronze_table(
                csv_file,
                date_str,
                target_directory,
                f"bronze_{file_prefix}",
                spark,
                date_filter_column="snapshot_date",
            )
    else:
        print("  ⚠ No snapshot_date column - processing once without date filter")
        # process only once since there's no date filtering
        utils.data_processing_bronze_table.process_bronze_table(
            csv_file,
            dates_str_lst[0],  # use first date as placeholder
            target_directory,
            f"bronze_{file_prefix}",
            spark,
            date_filter_column=None,
        )

# create silver datalake
silver_loan_daily_directory = "datamart/silver/loan_daily/"

if not os.path.exists(silver_loan_daily_directory):
    os.makedirs(silver_loan_daily_directory)

# run silver backfill (still only for loan data as per original logic)
for date_str in dates_str_lst:
    utils.data_processing_silver_table.process_silver_table_legacy(
        date_str, bronze_directory, silver_loan_daily_directory, spark
    )


# create bronze datalake
gold_label_store_directory = "datamart/gold/label_store/"

if not os.path.exists(gold_label_store_directory):
    os.makedirs(gold_label_store_directory)

# run gold backfill
for date_str in dates_str_lst:
    utils.data_processing_gold_table.process_labels_gold_table(
        date_str,
        silver_loan_daily_directory,
        gold_label_store_directory,
        spark,
        dpd=30,
        mob=6,
    )


folder_path = gold_label_store_directory
files_list = [
    folder_path + os.path.basename(f) for f in glob.glob(os.path.join(folder_path, "*"))
]
df = spark.read.option("header", "true").parquet(*files_list)
print("row_count:", df.count())

df.show()
