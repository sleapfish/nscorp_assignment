CREATE TABLE sjugo.nscorp_demo.raw_customer_table (
    CompanyName STRING,
    ContactName STRING,
    ContactTitle STRING,
    FullAddress STRUCT<Address: STRING, 
                       City: STRING, 
                       Country: STRING, 
                       PostalCode: BIGINT, 
                       Region: STRING>,
                       Phone STRING,
                       _CustomerID STRING) 
USING delta TBLPROPERTIES (delta.enableChangeDataFeed = true)