WITH _0 AS (
  SELECT DISTINCT
    UNNEST(STRING_TO_ARRAY(p_name, ' ')) AS color
  FROM tpch.part
)
SELECT
  COUNT(*) AS n
FROM _0
