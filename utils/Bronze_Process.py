import os
from datetime import datetime
from pathlib import Path
import pyspark.sql.functions as F
from pyspark.sql.functions import col

def resolve_assignment_csv(filename: str, base: str | Path | None = None) -> str:

    env = os.getenv("MLE_ASSIGNMENT_DIR")
    if env:
        p = Path(env)
        for root_name in ("Assignment", "MLE_Assignment"):
            cand = p / root_name / "data" / "data" / filename
            if cand.is_file():
                return cand.resolve().as_posix()

    def _search_up(start: Path, names=("Assignment", "MLE_Assignment")) -> Path | None:
        start = start.resolve()
        for cur in [start, *start.parents]:
            for nm in names:
                cand = cur / nm
                if cand.is_dir():
                    return cand
        return None

    if base is None:
        try:
            base = Path(__file__).resolve()
        except NameError:
            base = Path.cwd()
    hit = _search_up(Path(base)) or _search_up(Path.cwd())
    if not hit:
        raise FileNotFoundError("cannot find 'Assignment' (or 'MLE_Assignment'). "
                                "Set env MLE_ASSIGNMENT_DIR or pass a proper base.")
    csv_path = hit / "data" / "data" / filename
    if not csv_path.is_file():
        raise FileNotFoundError(f"cannot find file: {csv_path}")
    return csv_path.resolve().as_posix()

def process_bronze_table(snapshot_date_str: str,
                         bronze_lms_directory: str,
                         spark,
                         *,
                         file_name: str = "lms_loan_daily.csv",
                         save_parquet: bool = False) -> dict:

    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d")

    # path
    path = resolve_assignment_csv(file_name)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Data file not found: {path}")
    print(f"Loading: {path}")

    df = (spark.read
          .option("header", True)
          .option("inferSchema", True)
          .csv(path))

    if "snapshot_date" in df.columns:
        df = df.withColumn(
            "snapshot_date",
            F.coalesce(
                F.to_date("snapshot_date", "yyyy-MM-dd"),
                F.to_date("snapshot_date", "dd/MM/yyyy"),
                F.to_date("snapshot_date", "MM/dd/yyyy")
            )
        ).filter(F.col("snapshot_date") == F.to_date(F.lit(snapshot_date_str)))
    else:
        print(f"[INFO] 'snapshot_date' column not found, loaded {df.count()} rows without filtering.")

    dfs = {"loan_daily": df}


    # print("\nLOAN_DAILY SAMPLE:")
    # df.show(5, truncate=False)

    os.makedirs(bronze_lms_directory, exist_ok=True)
    tag = snapshot_date_str.replace("-", "_")

    if save_parquet:
        out_dir = os.path.join(bronze_lms_directory, f"bronze_loan_daily_{tag}")
        (df.write.mode("overwrite")
           .option("compression", "snappy")
           .parquet(out_dir))
        print(f"\n✓ Saved (parquet dir) to: {out_dir}")
    else:
        out_file = os.path.join(bronze_lms_directory, f"bronze_loan_daily_{tag}.csv")
        df.toPandas().to_csv(out_file, index=False)
        print(f"\n✓ Saved (single CSV) to: {out_file}")

    return dfs
