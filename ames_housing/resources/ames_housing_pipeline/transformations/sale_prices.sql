-- Create and populate the target table.

CREATE OR REFRESH STREAMING TABLE sale_prices
(
  CONSTRAINT valid_schema EXPECT (_rescued_data is not NULL) ON VIOLATION DROP ROW
)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'quality' = 'silver'
);

CREATE FLOW sales_prices_cdc AS AUTO CDC INTO
  sale_prices
FROM STREAM(bronze_cdf)
KEYS
  (property_id)
APPLY AS DELETE WHEN
  _change_type = "delete"
APPLY AS TRUNCATE WHEN
  _change_type = "truncate"
SEQUENCE BY
  (rcrd_timestamp, _commit_timestamp)
COLUMNS * EXCEPT
  (_change_type, _commit_version, _commit_timestamp, _rescued_data, rcrd_timestamp, ingest_time, order)
STORED AS
  SCD TYPE 1;


CREATE STREAMING TABLE sale_prices_quarantine 
  (
  CONSTRAINT invalid_schema EXPECT (_rescued_data is NULL) ON VIOLATION DROP ROW
)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'quality' = 'bronze'
) 
AS SELECT * FROM STREAM(bronze_cdf);