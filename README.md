# RB-Case-Study-Code-Snippet

### Objective Summary 
This case study focuses on transforming a hierarchical media asset metadata structure stored in a nested JSON format into a flat, analytics-ready table.
This solution is a simple prototype, designed specifically to meet the requirements of the case study. The focus is on data ingestion and handling semi-structured JSON data in Snowflake.

### Tools Used
AWS S3, IAM, Snowflake

### Files Included
DATA_INGESTION.sql : This script serves as the core solution to demonstrate how semi-structured data can be ingested, parsed, and made analytics-ready using SQL.

data_analysis.sql (Optional): This optional script showcases how the flattened taxonomy data can be used for generating insights.

### Steps Overview 
1. Create Storage Integration
A storage integration object (Snow_OBJ_2025) is defined in Snowflake to securely connect to an external S3 bucket using an AWS IAM role.

2. Describe Integration
A DESC INTEGRATION command is used to retrieve necessary metadata such as the IAM User ARN, which is later granted permission on the AWS side.

3. Create External Stage
A Snowflake stage (customer_stage) is configured to point to the S3 bucket, using static AWS key credentials for accessing the files.

4. Verify File Access
The LIST command is used to confirm visibility and access to the files stored in the S3 path.

5. Define File Format for JSON
A custom file format (my_json_format) is defined to handle NDJSON (Newline Delimited JSON) files.

6. Create Target Table
A Snowflake table (asset_metadata_taxonomy) is created to hold JSON data, including arrays for hierarchical and metadata values.

7. Load JSON Data
The COPY INTO command is used to load the .ndjson file from the S3 stage into the structured table using the defined JSON format.

8. Preview Loaded Data
A simple SELECT query with LIMIT is run to validate that the data has been successfully loaded into the table.

9. Flatten JSON Tree Structure
A CTE (tree_flattened) extracts hierarchical relationships by flattening the nested tree array, useful for path-based aggregations.

10. Extract Metadata Attributes
Another CTE (sap_extracted) is used to extract specific key-value pairs (e.g., sap) from the metadata array.

11. Final Aggregation Query
A SELECT query combines tree hierarchy and metadata by joining the CTEs, producing a clean output with an aggregated path and associated metadata.
