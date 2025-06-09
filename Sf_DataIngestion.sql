-- Step 1: Create a storage integration for accessing the S3 bucket
CREATE OR REPLACE STORAGE INTEGRATION Snow_OBJ_2025
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::586794473880:role/Snowflake_Role_2025'
  STORAGE_ALLOWED_LOCATIONS = ('s3://sriks3aws/');

-- Step 2: Describe the integration to retrieve ARN and other details
DESC INTEGRATION Snow_OBJ_2025;

-- Step 3: Create a Snowflake stage pointing to the S3 bucket using key-based credentials
CREATE OR REPLACE STAGE customer_stage
  URL = 's3://sriks3aws'
  CREDENTIALS = (
    AWS_KEY_ID = 'XXX' -- Removing the keys for security
    AWS_SECRET_KEY = 'YYY'  -- Removing the keys for security
  )
  FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);

-- Step 4: List files in the stage to verify S3 connectivity
LIST @customer_stage;

-- Step 5: Define a file format for JSON data loading
CREATE OR REPLACE FILE FORMAT my_json_format
  TYPE = 'JSON';

-- Step 6: Create a table to store JSON asset metadata taxonomy
CREATE OR REPLACE TABLE asset_metadata_taxonomy (
  optionset_id STRING,
  optionset_name STRING,
  option_id STRING,
  option_value STRING,
  height STRING,
  tree ARRAY,
  metadata ARRAY
);

-- Step 7: Load JSON data from the S3 stage into the target table
COPY INTO asset_metadata_taxonomy
FROM @customer_stage/taxonomy.ndjson
FILE_FORMAT = (TYPE = 'JSON')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Step 8: Preview the loaded data
SELECT * FROM asset_metadata_taxonomy 
LIMIT 100;

-- Step 9: Flatten the `tree` array and extract hierarchy for a specific option ID
WITH tree_flattened AS (
  SELECT
    t.option_id,
    t.option_value,
    tree_item.value:id::STRING AS tree_id,
    tree_item.value:depth::INT AS tree_depth
  FROM asset_metadata_taxonomy t,
       LATERAL FLATTEN(input => t.tree) AS tree_item
  WHERE t.option_id = 'TO-242WBGGCW1W11'
),

-- Step 10: Extract the `sap` value from the metadata array for the same option ID
sap_extracted AS (
  SELECT
    t.option_id,
    md.value:value::STRING AS sap
  FROM asset_metadata_taxonomy t,
       LATERAL FLATTEN(input => t.metadata) AS md
  WHERE md.value:key::STRING = 'sap'
    AND t.option_id = 'TO-242WBGGCW1W11'
)

-- Step 11: Aggregate the tree path and join with extracted sap value
SELECT
  tf.option_id,
  tf.option_value,
  LISTAGG(tf.tree_id, ' ; ') 
    WITHIN GROUP (ORDER BY tf.tree_depth) AS option_path,
  MAX(se.sap) AS sap
FROM tree_flattened tf
LEFT JOIN sap_extracted se
  ON tf.option_id = se.option_id
GROUP BY tf.option_id, tf.option_value, se.sap;
