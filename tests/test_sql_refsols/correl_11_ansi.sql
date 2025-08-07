WITH _t0 AS (
  SELECT DISTINCT
    p_brand
  FROM tpch.part
)
SELECT
  p_brand AS brand
FROM _t0
ORDER BY
  p_brand
