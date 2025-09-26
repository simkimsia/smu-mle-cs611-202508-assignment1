import argparse
import glob
import os

import pyspark

import utils.data_processing_bronze_table
import utils.data_processing_gold_table
import utils.data_processing_silver_table

# to call this script: python bronze_label_store.py --snapshotdate "2023-01-01" --datapath "data"


def main(snapshotdate, datapath=None):
    print("\n\n---starting job---\n\n")

    # Initialize SparkSession
    spark = (
        pyspark.sql.SparkSession.builder.appName("dev").master("local[*]").getOrCreate()
    )

    # Set log level to ERROR to hide warnings
    spark.sparkContext.setLogLevel("ERROR")

    # load arguments
    date_str = snapshotdate

    # set default data path to 'data' folder in same directory as script
    if datapath is None:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        datapath = os.path.join(script_dir, "data")

    print(f"Data source path: {datapath}")

    # list all CSV files in data folder
    csv_files = glob.glob(os.path.join(datapath, "*.csv"))
    print(f"Found CSV files: {[os.path.basename(f) for f in csv_files]}")

    if not csv_files:
        print("No CSV files found in data folder!")
        return

    # create bronze datalake
    bronze_directory = "datamart/bronze/"

    if not os.path.exists(bronze_directory):
        os.makedirs(bronze_directory)

    # process each CSV file
    for csv_file in csv_files:
        filename = os.path.basename(csv_file)
        file_prefix = os.path.splitext(filename)[0]

        print(f"\nProcessing: {filename}")

        # check if file has snapshot_date column
        try:
            sample_df = spark.read.csv(csv_file, header=True, inferSchema=True)
            has_snapshot_date = "snapshot_date" in sample_df.columns

            if has_snapshot_date:
                print("  ✓ Has snapshot_date column - processing with date filter")
                utils.data_processing_bronze_table.process_bronze_table(
                    csv_file,
                    date_str,
                    bronze_directory,
                    f"bronze_{file_prefix}",
                    spark,
                    date_filter_column="snapshot_date",
                )
            else:
                print("  ⚠ No snapshot_date column - processing without date filter")
                utils.data_processing_bronze_table.process_bronze_table(
                    csv_file,
                    date_str,
                    bronze_directory,
                    f"bronze_{file_prefix}",
                    spark,
                    date_filter_column=None,
                )
        except Exception as e:
            print(f"  ✗ Error processing {filename}: {str(e)}")
            continue

    # end spark session
    spark.stop()

    print("\n\n---completed job---\n\n")


if __name__ == "__main__":
    # Setup argparse to parse command-line arguments
    parser = argparse.ArgumentParser(description="run job")
    parser.add_argument("--snapshotdate", type=str, required=True, help="YYYY-MM-DD")
    parser.add_argument(
        "--datapath", type=str, help="Path to data folder (default: ./data)"
    )

    args = parser.parse_args()

    # Call main with arguments explicitly passed
    main(args.snapshotdate, args.datapath)
