# gold_sql_to_postgres.py
# Spark SQL → build GOLD (SCD2 dims + bridge + fact) and load to Postgres (JDBC)

import os
from pyspark.sql import SparkSession, functions as F

# ---------------- Spark session & perf ----------------
spark = (
    SparkSession.builder
    .appName("gold_sql_to_postgres")
    .master("local[*]")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "200"))
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ---------------- Paths & JDBC ----------------
SILVER = os.environ.get("SILVER_DIR", "data/silver")
PG_URL = os.environ.get("JDBC_URL", "jdbc:postgresql://postgres:5432/dwh")
PG_USER = os.environ.get("JDBC_USER", "pguser")
PG_PASS = os.environ.get("JDBC_PASS", "pgpass")
PG_SCHEMA = os.environ.get("PG_SCHEMA", "gold")

# Optional: only process a specific month (yyyyMM). If not set, process all.
PROCESS_YM = os.environ.get("PROCESS_YM")  # e.g., "202203"

def write_jdbc(df, table, partitions=16, batchsize=20000):
    (df.repartition(int(partitions))
       .write.format("jdbc")
       .option("url", PG_URL)
       .option("dbtable", f"{PG_SCHEMA}.{table}")
       .option("user", PG_USER)
       .option("password", PG_PASS)
       .option("driver", "org.postgresql.Driver")
       .option("stringtype", "unspecified")
       .option("batchsize", batchsize)
       .option("numPartitions", partitions)
       .option("truncate", "true")   # <<— TRUNCATE target table
       .mode("overwrite")            # <<— then insert fresh rows
       .save())

def read_silver(name, filter_ym=False):
    df = spark.read.parquet(f"{SILVER}/{name}")
    if filter_ym and PROCESS_YM and "p_ym" in df.columns:
        return df.where(F.col("p_ym") == PROCESS_YM)
    return df

# Prune by p_ym for time-series tables
acc   = read_silver("accounts",         filter_ym=True)       
acc_d = read_silver("account_details",  filter_ym=True)       
pprof = read_silver("person_profile",   filter_ym=True)       
piden = read_silver("person_iden",      filter_ym=True)       
pers  = read_silver("person",           filter_ym=False)      

acc.createOrReplaceTempView("accounts")
acc_d.createOrReplaceTempView("account_details")
pprof.createOrReplaceTempView("person_profile")
piden.createOrReplaceTempView("person_iden")
pers.createOrReplaceTempView("person")


# Dim_Date (distinct dates from all)
dim_date = spark.sql("""
SELECT DISTINCT dt
FROM (
  SELECT date AS dt FROM accounts
  UNION ALL
  SELECT date AS dt FROM account_details
  UNION ALL
  SELECT date AS dt FROM person_profile
  UNION ALL
  SELECT date AS dt FROM person_iden
) x
WHERE dt IS NOT NULL
""")

# Dim_Account SCD2
spark.sql("""
CREATE OR REPLACE TEMP VIEW account_timeline AS
SELECT /*+ BROADCAST(d) */
  a.acc_no,
  a.date,
  a.status,
  d.type
FROM accounts a
LEFT JOIN account_details d
  ON a.acc_no = d.acc_no AND a.date = d.date
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW dim_account_changes AS
SELECT
  acc_no,
  date AS effective_start_date,
  status,
  type,
  LAG(status) OVER (PARTITION BY acc_no ORDER BY date) AS prev_status,
  LAG(type)   OVER (PARTITION BY acc_no ORDER BY date) AS prev_type
FROM account_timeline
""")

dim_account_stg = spark.sql("""
SELECT
  acc_no,
  effective_start_date,
  status,
  type,
  LEAD(effective_start_date) OVER (PARTITION BY acc_no ORDER BY effective_start_date) AS next_start
FROM dim_account_changes
WHERE prev_status IS NULL OR status <> prev_status OR type <> prev_type
""").cache()

dim_account = dim_account_stg.selectExpr(
    "monotonically_increasing_id() as account_sk",
    "acc_no",
    "status",
    "type",
    "effective_start_date",
    "COALESCE(date_sub(next_start, 1), DATE '9999-12-31') as effective_end_date",
    "CASE WHEN next_start IS NULL THEN true ELSE false END as is_current"
)

# Dim_Person SCD2
spark.sql("""
CREATE OR REPLACE TEMP VIEW person_timeline AS
SELECT
  COALESCE(p1.person, p2.person) AS person,
  COALESCE(p1.date,   p2.date)   AS date,
  p1.name,
  p2.id
FROM person_profile p1
FULL OUTER JOIN person_iden p2
  ON p1.person = p2.person AND p1.date = p2.date
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW dim_person_changes AS
SELECT
  person,
  date AS effective_start_date,
  name,
  id,
  LAG(name) OVER (PARTITION BY person ORDER BY date) AS prev_name,
  LAG(id)   OVER (PARTITION BY person ORDER BY date) AS prev_id
FROM person_timeline
""")

dim_person_stg = spark.sql("""
SELECT
  person,
  effective_start_date,
  name,
  id,
  LEAD(effective_start_date) OVER (PARTITION BY person ORDER BY effective_start_date) AS next_start
FROM dim_person_changes
WHERE prev_name IS NULL OR name <> prev_name OR id <> prev_id
""").cache()

dim_person = dim_person_stg.selectExpr(
    "monotonically_increasing_id() as person_sk",
    "person",
    "name",
    "id",
    "effective_start_date",
    "COALESCE(date_sub(next_start, 1), DATE '9999-12-31') as effective_end_date",
    "CASE WHEN next_start IS NULL THEN true ELSE false END as is_current"
)

# Bridge
bridge = spark.sql("""
SELECT DISTINCT acc_no, person FROM person
""")

# Fact (date × account) + correct account_sk by date range
fact_base = spark.sql("""
SELECT DISTINCT date AS snapshot_date, acc_no
FROM accounts
WHERE date IS NOT NULL
""")

dim_account_for_join = dim_account.select("account_sk","acc_no","effective_start_date","effective_end_date")
fact = (
    fact_base.alias("f")
    .join(
        dim_account_for_join.alias("d"),
        (F.col("f.acc_no") == F.col("d.acc_no")) &
        (F.col("f.snapshot_date").between(F.col("d.effective_start_date"), F.col("d.effective_end_date"))),
        "left"
    )
    .select(F.col("f.snapshot_date"), F.col("f.acc_no"), F.col("d.account_sk"))
)

print("Writing GOLD to Postgres…")

write_jdbc(dim_account, "dim_account", partitions=16, batchsize=20000)
write_jdbc(dim_person,  "dim_person",  partitions=16, batchsize=20000)
write_jdbc(bridge,      "bridge_account_person", partitions=8,  batchsize=20000)
write_jdbc(dim_date,    "dim_date",    partitions=8,  batchsize=20000)
write_jdbc(fact,        "fact_account_snapshot", partitions=16, batchsize=20000)

print("GOLD built with Spark SQL and loaded to Postgres.")
spark.stop()
