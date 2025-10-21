from pyspark.sql import SparkSession, functions as F, types as T
import os

# ----------------------------
# Spark session & performance
# ----------------------------
spark = (
    SparkSession.builder
    .appName("bronze_to_silver_optimized")
    .master("local[*]")
    # Adaptive execution & shuffle tuning
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .config("spark.sql.shuffle.partitions", "200")        # tune to your cluster
    .config("spark.sql.files.maxPartitionBytes", 134217728)  # 128MB
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

BRONZE = "data/bronze_samples"
SILVER = "data/silver"

# ----------------------------
# Explicit input schemas
# ----------------------------
schema_accounts = T.StructType([
    T.StructField("Acc no", T.StringType(), True),
    T.StructField("Date",   T.StringType(), True),
    T.StructField("Status", T.StringType(), True),
])

schema_account_details = T.StructType([
    T.StructField("Acc no", T.StringType(), True),
    T.StructField("Date",   T.StringType(), True),
    T.StructField("type",   T.StringType(), True),
])

schema_person = T.StructType([
    T.StructField("Acc no", T.StringType(), True),
    T.StructField("Person", T.StringType(), True),
])

schema_person_profile = T.StructType([
    T.StructField("Person", T.StringType(), True),
    T.StructField("Name",   T.StringType(), True),
    T.StructField("Date",   T.StringType(), True),
])

schema_person_iden = T.StructType([
    T.StructField("Person", T.StringType(), True),
    T.StructField("Id",     T.StringType(), True),
    T.StructField("Date",   T.StringType(), True),
])

def read_csv(name, schema):
    return (
        spark.read
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .csv(f"{BRONZE}/{name}.csv")
    )

def read_parquet_if_exists(name):
    path = f"{SILVER}/{name}"
    return spark.read.parquet(path) if os.path.exists(path) else None

def write_partitioned(df, name, partitions, mode="overwrite"):
    # Control small files; repartition on partition columns.
    df = df.repartition(*partitions)
    (
        df.write
          .mode(mode)
          .partitionBy(*partitions)
          .parquet(f"{SILVER}/{name}")
    )

# Fast multi-format date parser (no UDFs)
def parse_date(col):
    # Extend formats if needed
    return F.coalesce(
        F.to_date(col, "dd-MMM-yy"),
        F.to_date(col, "yyyy-MM-dd"),
        F.to_date(col, "dd/MM/yyyy"),
        F.to_date(col, "MM/dd/yyyy")
    )

# Add a year-month partition column (yyyyMM)
def with_ym(df, date_col="date"):
    return df.withColumn("p_ym", F.date_format(F.col(date_col), "yyyyMM"))

# CDC merge that preserves FULL HISTORY:
# union existing + new; drop exact duplicates by composite key
def merge_full_history(df_new, silver_name, key_cols, partitions):
    df_existing = read_parquet_if_exists(silver_name)
    if df_existing is None:
        print(f"🆕 Initial full load → {silver_name}")
        write_partitioned(df_new, silver_name, partitions, mode="overwrite")
        return
    print(f"🔁 Incremental CDC merge → {silver_name} on keys {key_cols}")
    merged = (
        df_existing.select(df_new.columns)  # align cols/order
                  .unionByName(df_new)
                  .dropDuplicates(key_cols)
    )
    write_partitioned(merged, silver_name, partitions, mode="overwrite")

# ----------------------------
# Normalize each table (Bronze → curated DF)
# ----------------------------

# ACCOUNTS (keep full history acc_no+date)
accounts_bronze = read_csv("accounts", schema_accounts)
accounts = (
    accounts_bronze
    .withColumn("acc_no", F.col("Acc no"))
    .withColumn("date",   parse_date(F.col("Date")))
    .withColumn("status", F.col("Status"))
    .select("acc_no", "date", "status")
    .filter(F.col("acc_no").isNotNull() & F.col("date").isNotNull())
)
accounts = with_ym(accounts, "date")

# ACCOUNT DETAILS (keep full history acc_no+date)
account_details_bronze = read_csv("account_details", schema_account_details)
account_details = (
    account_details_bronze
    .withColumn("acc_no", F.col("Acc no"))
    .withColumn("date",   parse_date(F.col("Date")))
    .withColumn("type",   F.col("type"))
    .select("acc_no", "date", "type")
    .filter(F.col("acc_no").isNotNull() & F.col("date").isNotNull())
)
account_details = with_ym(account_details, "date")

# PERSON (relationship; no date)
person_bronze = read_csv("person", schema_person)
person = (
    person_bronze
    .withColumn("acc_no", F.col("Acc no"))
    .withColumn("person", F.col("Person"))
    .select("acc_no", "person")
    .filter(F.col("acc_no").isNotNull() & F.col("person").isNotNull())
    .dropDuplicates(["acc_no", "person"])  # natural de-dupe
)
# partition by entity key to prune joins
person = person.repartition("acc_no")

# PERSON PROFILE (full history person+date)
person_profile_bronze = read_csv("person_profile", schema_person_profile)
person_profile = (
    person_profile_bronze
    .withColumn("person", F.col("Person"))
    .withColumn("name",   F.col("Name"))
    .withColumn("date",   parse_date(F.col("Date")))
    .select("person", "name", "date")
    .filter(F.col("person").isNotNull() & F.col("date").isNotNull())
)
person_profile = with_ym(person_profile, "date")

# PERSON IDEN (full history person+id+date)
person_iden_bronze = read_csv("person_iden", schema_person_iden)
person_iden = (
    person_iden_bronze
    .withColumn("person", F.col("Person"))
    .withColumn("id",     F.col("Id"))
    .withColumn("date",   parse_date(F.col("Date")))
    .select("person", "id", "date")
    .filter(F.col("person").isNotNull() & F.col("id").isNotNull() & F.col("date").isNotNull())
)
person_iden = with_ym(person_iden, "date")

# ----------------------------
# CDC merges (FULL HISTORY)
# ----------------------------
merge_full_history(
    accounts,
    silver_name="accounts",
    key_cols=["acc_no", "date", "status"],   # keep every dated status row
    partitions=["p_ym"]                      # date-pruned reads (better than per-acc partition explosion)
)
merge_full_history(
    account_details,
    silver_name="account_details",
    key_cols=["acc_no", "date", "type"],
    partitions=["p_ym"]
)
merge_full_history(
    person,
    silver_name="person",
    key_cols=["acc_no", "person"],
    partitions=["acc_no"]                    # prune joins by account
)
merge_full_history(
    person_profile,
    silver_name="person_profile",
    key_cols=["person", "date", "name"],
    partitions=["p_ym"]
)
merge_full_history(
    person_iden,
    silver_name="person_iden",
    key_cols=["person", "id", "date"],
    partitions=["p_ym"]
)

print("✅ Bronze → Silver (optimized, full-history CDC) completed.")
