# Databricks notebook source
from pyspark.sql.functions import count, col

# COMMAND ----------

# Databricks notebook parameters
dbutils.widgets.text("sf_catalog", "snowflake_bt_prod")
dbutils.widgets.text("sf_schema", "sales")
dbutils.widgets.text("sf_table", "CLARI_SERVICES_EXPORT")
dbutils.widgets.text("sf_pk", "USER, EMAIL, CRM_USER_ID, TIMEFRAMEID, FIELD, START_DAY, END_DAY, DATA_TYPE")

dbutils.widgets.text("db_catalog", "ent_dev_stage")
dbutils.widgets.text("db_schema", "raw")
dbutils.widgets.text("db_table", "clari_fc_services")
dbutils.widgets.text("db_pk", "USER, EMAIL, CRM_USER_ID, TIMEFRAMEID, FIELD, START_DAY, END_DAY, DATA_TYPE")

# Retrieve the parameters
sf_catalog = dbutils.widgets.get("sf_catalog")
sf_schema = dbutils.widgets.get("sf_schema")
sf_table = dbutils.widgets.get("sf_table")
sf_pk = [col.strip() for col in dbutils.widgets.get("sf_pk").split(",")]

db_catalog = dbutils.widgets.get("db_catalog")
db_schema = dbutils.widgets.get("db_schema")
db_table = dbutils.widgets.get("db_table")
db_pk = [col.strip() for col in dbutils.widgets.get("db_pk").split(",")]


# COMMAND ----------

# Load Snowflake table
sf_df = spark.read.table(f"{sf_catalog}.{sf_schema}.{sf_table}")

# Load Databricks table
db_df = spark.read.table(f"{db_catalog}.{db_schema}.{db_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC Row Count Comparison Block

# COMMAND ----------

try:
    sf_count = sf_df.count()
    db_count = db_df.count()
    
    print(f"✅ Snowflake table row count     : {sf_count}")
    print(f"✅ Databricks table row count    : {db_count}")

    if sf_count == db_count:
        print("✅ Row counts match!")
    else:
        print("❗ Row count mismatch detected!")
        print(f"   ➤ Difference: {abs(sf_count - db_count)} rows")
        print(f"   ➤ Snowflake - Databricks = {sf_count - db_count}")
except Exception as e:
    print("❌ Error while comparing row counts.")
    print(f"Details: {e}")


# COMMAND ----------

# MAGIC %md
# MAGIC Primary Key Duplicate Check

# COMMAND ----------

# Load Snowflake and Databricks tables
sf_df = spark.read.table(f"{sf_catalog}.{sf_schema}.{sf_table}")
db_df = spark.read.table(f"{db_catalog}.{db_schema}.{db_table}")

# ---- Check for duplicates in Databricks table ----
print(f"\n🔍 Checking for duplicates in Databricks table: {db_catalog}.{db_schema}.{db_table}")
if not db_pk:
    raise ValueError("❌ Primary key columns are not specified for Databricks table.")

missing_cols_db = [col for col in db_pk if col not in db_df.columns]
if missing_cols_db:
    raise ValueError(f"❌ Missing PK columns in Databricks table: {missing_cols_db}")

try:
    db_duplicates = (
        db_df.groupBy(*db_pk)
             .agg(count("*").alias("duplicate_count"))
             .filter("duplicate_count > 1")
    )

    db_dup_count = db_duplicates.count()
    if db_dup_count > 0:
        print(f"❗ Found {db_dup_count} duplicate PKs in Databricks.")
        display(db_duplicates)
    else:
        print("✅ No duplicate primary keys in Databricks table.")

except Exception as e:
    print("❌ Error checking Databricks PKs.")
    print(f"Details: {e}")

# ---- Check for duplicates in Snowflake table ----
print(f"\n🔍 Checking for duplicates in Snowflake table: {sf_catalog}.{sf_schema}.{sf_table}")
if not sf_pk:
    raise ValueError("❌ Primary key columns are not specified for Snowflake table.")

missing_cols_sf = [col for col in sf_pk if col not in sf_df.columns]
if missing_cols_sf:
    raise ValueError(f"❌ Missing PK columns in Snowflake table: {missing_cols_sf}")

try:
    sf_duplicates = (
        sf_df.groupBy(*sf_pk)
             .agg(count("*").alias("duplicate_count"))
             .filter("duplicate_count > 1")
    )

    sf_dup_count = sf_duplicates.count()
    if sf_dup_count > 0:
        print(f"❗ Found {sf_dup_count} duplicate PKs in Snowflake.")
        display(sf_duplicates)
    else:
        print("✅ No duplicate primary keys in Snowflake table.")

except Exception as e:
    print("❌ Error checking Snowflake PKs.")
    print(f"Details: {e}")


# COMMAND ----------

# MAGIC %md
# MAGIC Snowflake ➖ Databricks

# COMMAND ----------

# --- Validate primary key configuration ---
if not sf_pk or not db_pk:
    raise ValueError("❌ Primary key columns must be specified for both Snowflake and Databricks.")

if len(sf_pk) != len(db_pk):
    raise ValueError("❌ Snowflake and Databricks PK column counts do not match. Ensure 1:1 mapping.")

# --- Validate column presence ---
missing_sf_cols = [col for col in sf_pk if col not in sf_df.columns]
missing_db_cols = [col for col in db_pk if col not in db_df.columns]

if missing_sf_cols:
    raise ValueError(f"❌ Missing PK columns in Snowflake table: {missing_sf_cols}")
if missing_db_cols:
    raise ValueError(f"❌ Missing PK columns in Databricks table: {missing_db_cols}")

# --- Select and rename PK columns for comparison ---
sf_pk_df = sf_df.select(*sf_pk)

# Create a renamed version of db_df with PKs matching Snowflake PK names
db_pk_renamed = db_df.select([col(db_pk[i]).alias(sf_pk[i]) for i in range(len(sf_pk))])

# --- Perform subtract (Snowflake - Databricks) ---
try:
    print(f"\n🔍 Comparing rows on PKs: Snowflake ➖ Databricks")
    sf_minus_db = sf_pk_df.subtract(db_pk_renamed)
    row_count = sf_minus_db.count()

    if row_count > 0:
        print(f"❗ Found {row_count} unmatched PK rows in Snowflake (not found in Databricks).")
        display(sf_minus_db)
    else:
        print("✅ All Snowflake PK rows are present in Databricks.")

except Exception as e:
    print("❌ Error during Snowflake ➖ Databricks comparison.")
    print(f"Details: {e}")


# COMMAND ----------

# MAGIC %md
# MAGIC Databricks ➖ Snowflake

# COMMAND ----------

# --- Validate primary key configuration ---
if not sf_pk or not db_pk:
    raise ValueError("❌ Primary key columns must be specified for both Snowflake and Databricks.")

if len(sf_pk) != len(db_pk):
    raise ValueError("❌ Snowflake and Databricks PK column counts do not match. Ensure 1:1 mapping.")

# --- Validate column presence ---
missing_sf_cols = [col for col in sf_pk if col not in sf_df.columns]
missing_db_cols = [col for col in db_pk if col not in db_df.columns]

if missing_sf_cols:
    raise ValueError(f"❌ Missing PK columns in Snowflake table: {missing_sf_cols}")
if missing_db_cols:
    raise ValueError(f"❌ Missing PK columns in Databricks table: {missing_db_cols}")

# --- Select and rename PK columns for comparison ---
db_pk_df = db_df.select(*db_pk)

# Rename Snowflake PK columns to match Databricks PK names for comparison
sf_pk_renamed = sf_df.select([col(sf_pk[i]).alias(db_pk[i]) for i in range(len(db_pk))])

# --- Perform subtract (Databricks - Snowflake) ---
try:
    print(f"\n🔍 Comparing rows on PKs: Databricks ➖ Snowflake")
    db_minus_sf = db_pk_df.subtract(sf_pk_renamed)
    row_count = db_minus_sf.count()

    if row_count > 0:
        print(f"❗ Found {row_count} unmatched PK rows in Databricks (not found in Snowflake).")
        display(db_minus_sf)
    else:
        print("✅ All Databricks PK rows are present in Snowflake.")

except Exception as e:
    print("❌ Error during Databricks ➖ Snowflake comparison.")
    print(f"Details: {e}")
