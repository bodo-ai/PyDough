WITH _t0 AS (
  SELECT DISTINCT
    LOWER(n_name) AS lower_n_name
  FROM tpch.nation
)
SELECT
  lower_n_name AS name
FROM _t0
ORDER BY
  1 NULLS FIRST
