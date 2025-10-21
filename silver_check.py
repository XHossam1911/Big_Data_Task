from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder
    .appName("test_silver_results")
    .master("local[*]")
    .getOrCreate()
)

silver = "data/silver"

tables = [
    "accounts",
    "account_details",
    "person",
    "person_profile",
    "person_iden"
]

def show_summary(df, name, key_cols=None, date_col=None):
    print(f"\n=== {name.upper()} ===")

    # total rows
    print(f"Total rows: {df.count()}")

    # distinct keys
    if key_cols:
        print(f"Distinct {key_cols}: {df.select(*key_cols).distinct().count()}")

    # duplicates
    if key_cols:
        dupes = (
            df.groupBy(*key_cols)
              .count()
              .filter(F.col("count") > 1)
        )
        dupes_count = dupes.count()
        if dupes_count > 0:
            print(f"⚠️ Found {dupes_count} duplicate key(s):")
            dupes.show(truncate=False)
        else:
            print("✅ No duplicates by key columns.")

    # date range
    if date_col and date_col in df.columns:
        df.select(
            F.min(date_col).alias("min_date"),
            F.max(date_col).alias("max_date")
        ).show()

    # sample data
    print("Sample rows:")
    df.orderBy(*[F.col(c) for c in (key_cols or df.columns)]).show(5, truncate=False)


# === Test each table ===
for name in tables:
    path = f"{silver}/{name}"
    print(f"\n📂 Reading {path}")
    df = spark.read.parquet(path)

    if name == "accounts":
        show_summary(df, name, key_cols=["acc_no"], date_col="date")
    elif name == "account_details":
        show_summary(df, name, key_cols=["acc_no"], date_col="date")
    elif name == "person":
        show_summary(df, name, key_cols=["acc_no", "person"])
    elif name == "person_profile":
        show_summary(df, name, key_cols=["person"], date_col="date")
    elif name == "person_iden":
        show_summary(df, name, key_cols=["person"], date_col="date")
    else:
        show_summary(df, name)

spark.stop()
print("\n✅ Silver validation complete.")
