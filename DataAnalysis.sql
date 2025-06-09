-- Creating a view from transformed query for data analysis

CREATE OR REPLACE VIEW v_flattened_asset_taxonomy AS
WITH tree_flattened AS (
  SELECT
    t.option_id,
    t.option_value,
    tree_item.value:id::STRING AS tree_id,
    tree_item.value:depth::INT AS tree_depth
  FROM asset_metadata_taxonomy t,
       LATERAL FLATTEN(input => t.tree) AS tree_item
),

sap_extracted AS (
  SELECT
    t.option_id,
    md.value:value::STRING AS sap
  FROM asset_metadata_taxonomy t,
       LATERAL FLATTEN(input => t.metadata) AS md
  WHERE md.value:key::STRING = 'sap'
)

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

SELECT * FROM v_flattened_asset_taxonomy;

--INSIGHTS

-- 1.Count of Entries Per Option Value: Helps identify the most commonly used asset types.
SELECT option_value, COUNT(*) AS entry_count
FROM v_flattened_asset_taxonomy
GROUP BY option_value
ORDER BY entry_count DESC;


--2.Top-Level Categories Based on First Path Element
SELECT 
  SPLIT_PART(option_path, ' ; ', 1) AS top_level_id,
  COUNT(*) AS total
FROM v_flattened_asset_taxonomy
GROUP BY top_level_id
ORDER BY total DESC;

--3. Missing or Invalid SAP Patterns:  Ensures sap values follow naming standards like SPOPxxxxxx.
SELECT *
FROM v_flattened_asset_taxonomy
WHERE sap IS NOT NULL
AND sap NOT ILIKE 'SPOP%';
