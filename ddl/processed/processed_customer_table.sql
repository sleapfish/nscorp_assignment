CREATE TABLE sjugo.nscorp_demo.processed_customer_table (
    CompanyName STRING,
    ContactName STRING,
    ContactTitle STRING,
    `FullAddress.Address` STRING, 
    `FullAddress.City` STRING, 
    `FullAddress.Country` STRING, 
    `FullAddress.PostalCode` BIGINT, 
    `FullAddress.Region` STRING,
    Phone STRING,
    _CustomerID STRING,
    Date STRING,
    meta_original_source STRING,
    meta_source_table STRING,
    meta_pipeline_name STRING,
    meta_pii_flag BOOLEAN,
    meta_data_classification STRING)
USING delta
PARTITIONED BY (Date)