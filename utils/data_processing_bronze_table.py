import os
from datetime import datetime

from pyspark.sql.functions import col


def process_bronze_table(
    file_path, snapshot_date_str, output_directory, output_prefix, spark, date_filter_column="snapshot_date"
):
    # prepare arguments
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d")

    # load data - IRL ingest from back end source system
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    # apply date filtering if the column exists
    if date_filter_column in df.columns:
        # Convert snapshot_date column to standard date format for comparison
        # Handle both yyyy-MM-dd and d/M/yy formats
        from pyspark.sql.functions import to_date, coalesce
        df = df.withColumn(
            date_filter_column,
            coalesce(
                to_date(col(date_filter_column), 'yyyy-MM-dd'),
                to_date(col(date_filter_column), 'd/M/yy')
            )
        )
        df = df.filter(col(date_filter_column) == snapshot_date_str)

    print(f"{file_path} - {snapshot_date_str} row count: {df.count()}")

    # save bronze table to datamart - IRL connect to database to write
    # create output directory if it doesn't exist
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    partition_name = f"{output_prefix}_{snapshot_date_str.replace('-', '_')}.csv"
    filepath = os.path.join(output_directory, partition_name)
    df.toPandas().to_csv(filepath, index=False)
    print(f"saved to: {filepath}")

    return df
