import argparse
import glob
import os
from datetime import datetime

import pyspark

import utils.data_processing_bronze_table
import utils.data_processing_gold_table
import utils.data_processing_silver_table

# Parse command line arguments
parser = argparse.ArgumentParser(
    description="Run ETL pipeline with optional early exit"
)
parser.add_argument(
    "--bronze-only",
    action="store_true",
    help="Exit after bronze table processing (for testing bronze layer)",
)
parser.add_argument(
    "--timestamp",
    type=str,
    help="Use existing timestamp directory (format: YYYYMMDD_HHMMSS)",
)
parser.add_argument(
    "--layer",
    choices=["bronze", "silver", "gold"],
    help="Start processing from specific layer (requires --timestamp)",
)
args = parser.parse_args()

# Validate arguments
if args.layer and not args.timestamp:
    parser.error("--layer requires --timestamp to be specified")

# Generate or use provided timestamp for directory structure
if args.timestamp:
    timestamp = args.timestamp
    print(f"📁 Using existing timestamp: {timestamp}")
else:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    print(f"📁 Created new timestamp: {timestamp}")

# Create base datamart directory with timestamp
base_datamart_dir = f"datamart/{timestamp}/"
bronze_directory = f"{base_datamart_dir}bronze/"
silver_directory = f"{base_datamart_dir}silver/"
gold_directory = f"{base_datamart_dir}gold/"

print(f"📂 Base datamart directory: {base_datamart_dir}")

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

# Determine which layers to run based on arguments
run_bronze = not args.layer or args.layer == "bronze"
run_silver = not args.layer or args.layer in ["bronze", "silver"]
run_gold = not args.layer or args.layer in ["bronze", "silver", "gold"]

print("🔄 Pipeline execution plan:")
print(f"  Bronze: {'✓' if run_bronze else '⏩ Skip'}")
print(f"  Silver: {'✓' if run_silver else '⏩ Skip'}")
print(f"  Gold:   {'✓' if run_gold else '⏩ Skip'}")

# Bronze layer processing
if run_bronze:
    print("🥉 Processing Bronze layer...")

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
        "lms_loan_daily.csv": f"{bronze_directory}lms/",
        "features_attributes.csv": f"{bronze_directory}features/attributes",
        "features_financials.csv": f"{bronze_directory}features/financials",
        "feature_clickstream.csv": f"{bronze_directory}features/clickstream",
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

    print("✅ Bronze table processing completed!")
else:
    print("⏩ Skipping Bronze layer processing")

# Exit early if bronze-only flag is set
if args.bronze_only:
    print("🛑 Exiting after bronze processing as requested (--bronze-only flag)")
    spark.stop()
    exit(0)

# Silver layer processing
if run_silver:
    print("🥈 Processing Silver layer...")

    # Create directory structure for all silver tables
    silver_loan_daily_directory = f"{silver_directory}loan_daily/"
    silver_clickstream_directory = f"{silver_directory}clickstream/"
    silver_attributes_directory = f"{silver_directory}attributes/"
    silver_financials_directory = f"{silver_directory}financials/"

    for directory in [silver_loan_daily_directory, silver_clickstream_directory,
                     silver_attributes_directory, silver_financials_directory]:
        if not os.path.exists(directory):
            os.makedirs(directory)

    # Process loan daily data (existing logic)
    print("  📊 Processing loan daily silver tables...")
    bronze_lms_directory = f"{bronze_directory}lms/"
    for date_str in dates_str_lst:
        utils.data_processing_silver_table.process_silver_table_legacy(
            date_str, bronze_lms_directory, silver_loan_daily_directory, spark
        )

    # Process clickstream data
    print("  🖱️ Processing clickstream silver tables...")
    bronze_clickstream_directory = f"{bronze_directory}features/clickstream/"
    for date_str in dates_str_lst:
        bronze_filepath = f"{bronze_clickstream_directory}bronze_feature_clickstream_{date_str.replace('-', '_')}.csv"
        silver_filepath = f"{silver_clickstream_directory}silver_clickstream_{date_str.replace('-', '_')}.parquet"

        # Check if bronze file exists before processing
        if os.path.exists(bronze_filepath):
            utils.data_processing_silver_table.process_silver_table(
                'clickstream', bronze_filepath, silver_filepath, spark
            )

    # Process attributes data
    print("  👤 Processing attributes silver tables...")
    bronze_attributes_directory = f"{bronze_directory}features/attributes/"
    for date_str in dates_str_lst:
        bronze_filepath = f"{bronze_attributes_directory}bronze_features_attributes_{date_str.replace('-', '_')}.csv"
        silver_filepath = f"{silver_attributes_directory}silver_attributes_{date_str.replace('-', '_')}.parquet"

        # Check if bronze file exists before processing
        if os.path.exists(bronze_filepath):
            utils.data_processing_silver_table.process_silver_table(
                'attributes', bronze_filepath, silver_filepath, spark
            )

    # Process financials data
    print("  💰 Processing financials silver tables...")
    bronze_financials_directory = f"{bronze_directory}features/financials/"
    for date_str in dates_str_lst:
        bronze_filepath = f"{bronze_financials_directory}bronze_features_financials_{date_str.replace('-', '_')}.csv"
        silver_filepath = f"{silver_financials_directory}silver_financials_{date_str.replace('-', '_')}.parquet"

        # Check if bronze file exists before processing
        if os.path.exists(bronze_filepath):
            utils.data_processing_silver_table.process_silver_table(
                'financials', bronze_filepath, silver_filepath, spark
            )

    print("✅ Silver table processing completed!")
else:
    print("⏩ Skipping Silver layer processing")


# Gold layer processing
if run_gold:
    print("🥇 Processing Gold layer...")

    gold_label_store_directory = f"{gold_directory}label_store/"
    gold_feature_store_directory = f"{gold_directory}feature_store/"

    for directory in [gold_label_store_directory, gold_feature_store_directory]:
        if not os.path.exists(directory):
            os.makedirs(directory)

    # Process labels (existing)
    print("  🏷️ Processing label store...")
    for date_str in dates_str_lst:
        utils.data_processing_gold_table.process_labels_gold_table(
            date_str,
            silver_loan_daily_directory
            if run_silver
            else f"{silver_directory}loan_daily/",
            gold_label_store_directory,
            spark,
            dpd=30,
            mob=6,
        )

    # Process features (new)
    print("  🎯 Processing feature store...")
    silver_dirs = {
        'loan_daily': f"{silver_directory}loan_daily/",
        'attributes': f"{silver_directory}attributes/",
        'financials': f"{silver_directory}financials/",
        'clickstream': f"{silver_directory}clickstream/"
    }

    for date_str in dates_str_lst:
        utils.data_processing_gold_table.process_features_gold_table(
            date_str, silver_dirs, gold_feature_store_directory, spark
        )

    folder_path = gold_label_store_directory
    files_list = [
        folder_path + os.path.basename(f)
        for f in glob.glob(os.path.join(folder_path, "*"))
    ]
    df = spark.read.option("header", "true").parquet(*files_list)
    print("row_count:", df.count())

    df.show()

    print("✅ Gold table processing completed!")
else:
    print("⏩ Skipping Gold layer processing")

print(f"\n🎉 Pipeline completed! Results stored in: {base_datamart_dir}")
spark.stop()
