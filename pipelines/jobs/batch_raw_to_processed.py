from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date
from pyspark.sql.utils import AnalysisException
from pipelines.transformations import flatten, lineage
from delta.tables import *
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

def _get_last_successful_timestmap(table_name):
  timestamp = spark.read.format("delta") \
                .table("sjugo.nscorp_demo.etl_control_table") \
                .filter(col("table_name") == table_name ) \
                .select("last_successful_timestamp") \
                .first()["last_successful_timestamp"]

  return timestamp

def load_delta_cdc(table_name):
  skip_load = False
  df = None
  try:
    df = spark.read.format("delta") \
            .option("readChangeFeed", "true") \
            .option("startingTimestamp", _get_last_successful_timestmap(table_name)) \
            .table("sjugo.nscorp_demo."+table_name)
  except AnalysisException as e:
    skip_load = True

  return df, skip_load

def drop_duplicates(df, key):
  return df.dropDuplicates(key)

def flatten_df(df):
  return flatten.flatten_dataframe(df)

def drop_delta_cdf_columns(df):
  return df.drop("_change_type", "_commit_version", "_commit_timestamp")

def add_metadata_fields(df):
  df = lineage.add_lineage_fields(
    df=df,
    meta_original_source = "kafka",
    meta_source_table = "raw_customer_table",
    meta_pipeline_name = "batch_raw_to_processed",
    pii_flag = False,
    data_classification = "Public"
  )
  return df

def add_partition_field(df, current_ts):
  df = df.withColumn("Date", lit(current_ts.strftime("%Y-%m-%d")))
  return df

def load_processed_table(df, table_name, merge_condition):
  update_cols_dict = { 
    "CompanyName" : "src.CompanyName",
    "ContactName" : "src.ContactName",
    "ContactTitle" : "src.ContactTitle",
    "`FullAddress.Address`" : "src.`FullAddress.Address`",
    "`FullAddress.City`" : "src.`FullAddress.City`",
    "`FullAddress.Country`" : "src.`FullAddress.Country`",
    "`FullAddress.PostalCode`" : "src.`FullAddress.PostalCode`",
    "`FullAddress.Region`" : "src.`FullAddress.Region`",
    "Phone": "src.Phone"
  }

  insert_cols_dict = { 
    "CompanyName" : "src.CompanyName",
    "ContactName" : "src.ContactName",
    "ContactTitle" : "src.ContactTitle",
    "`FullAddress.Address`" : "src.`FullAddress.Address`",
    "`FullAddress.City`" : "src.`FullAddress.City`",
    "`FullAddress.Country`" : "src.`FullAddress.Country`",
    "`FullAddress.PostalCode`" : "src.`FullAddress.PostalCode`",
    "`FullAddress.Region`" : "src.`FullAddress.Region`",
    "Phone" : "src.Phone",
    "_CustomerID" : "src._CustomerID",
    "Date" : "src.Date",
    "meta_original_source" : "src.meta_original_source",
    "meta_source_table" : "src.meta_source_table",
    "meta_pipeline_name" : "src.meta_pipeline_name",
    "meta_pii_flag" : "src.meta_pii_flag",
    "meta_data_classification" : "src.meta_data_classification"
  }
                      
  delta_table = DeltaTable.forName(spark, table_name)
  delta_table.alias("tgt").merge(
    df.alias("src"),
    "{merge_condition}".format(merge_condition=merge_condition)) \
  .whenMatchedUpdate(set = update_cols_dict ) \
  .whenNotMatchedInsert(values = insert_cols_dict) \
  .execute()

def update_etl_control_table(table_name, current_ts):
  etl_control_table = DeltaTable.forName(spark, "sjugo.nscorp_demo.etl_control_table")
  etl_control_table.update(col("table_name") == table_name, { "last_successful_timestamp": lit(current_ts) } )

def etl():
  current_ts = datetime.now()
  df, skip_load = load_delta_cdc("raw_customer_table")
  if not skip_load:
    df = drop_duplicates(df, ["_CustomerID"])
    df = flatten_df(df)
    df = drop_delta_cdf_columns(df)
    df = add_metadata_fields(df)
    df = add_partition_field(df, current_ts)
    df = load_processed_table(df, "sjugo.nscorp_demo.processed_customer_table", "src._CustomerID = tgt._CustomerID")
    update_etl_control_table("raw_customer_table", current_ts)