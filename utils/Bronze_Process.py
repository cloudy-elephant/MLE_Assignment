import os
from datetime import datetime
from pathlib import Path
import pyspark.sql.functions as F
from pyspark.sql.functions import col


def process_bronze_table(snapshot_date_str, bronze_lms_directory, spark):
    """
    Process bronze table for a given snapshot date.

    Args:
        snapshot_date_str: Date string in 'YYYY-MM-DD' format
        bronze_lms_directory: Output directory for bronze tables
        spark: SparkSession instance

    Returns:
        Dictionary of DataFrames
    """
    # Parse snapshot date
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d")

    # Define data file paths - using absolute paths
    base_path = r"C:\Users\HP\Desktop\MLE\Assignment\data"

    paths = {
        # "clickstream": os.path.join(base_path, "feature_clickstream.csv"),
        # "attributes": os.path.join(base_path, "feature_attributes.csv"),
        # "financials": os.path.join(base_path, "feature_financials.csv"),
        "loan_daily": os.path.join(base_path, "data", "lms_loan_daily.csv"),
    }

    # Verify files exist
    for name, path in paths.items():
        if not os.path.exists(path):
            raise FileNotFoundError(f"Data file not found: {path}")

    # Load all datasets
    dfs = {}
    for name, path in paths.items():
        # print(f"Loading {name} from: {path}")
        df = (spark.read
              .option("header", True)
              .option("inferSchema", True)
              .csv(path))

        # Filter by snapshot_date if column exists
        if "snapshot_date" in df.columns:
            df = df.filter(F.col("snapshot_date") == F.lit(snapshot_date))
            # print(f"{name} - rows after filtering by {snapshot_date_str}: {df.count()}")
        else:
            print(f"{name} - no snapshot_date column, loaded {df.count()} rows")

        dfs[name] = df

    # Display sample data
    # print("\n" + "=" * 50)
    # print("Sample Data Preview:")
    # print("=" * 50)
    for name, df in dfs.items():
        print(f"\n{name.upper()}:")
        df.show(5, truncate=False)

    # Save bronze table (loan_daily) to datamart
    df_loan_daily = dfs["loan_daily"]
    partition_name = f"bronze_loan_daily_{snapshot_date_str.replace('-', '_')}.csv"
    filepath = os.path.join(bronze_lms_directory, partition_name)

    # Ensure output directory exists
    os.makedirs(bronze_lms_directory, exist_ok=True)

    # Save to CSV
    df_loan_daily.toPandas().to_csv(filepath, index=False)
    print(f"\n✓ Saved to: {filepath}")

    return dfs