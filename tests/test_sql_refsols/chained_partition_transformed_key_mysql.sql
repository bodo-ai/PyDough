WITH _t0 AS (
  SELECT DISTINCT
    LOWER(n_name) AS lower_n_name
  FROM tpch.NATION
)
SELECT
  lower_n_name COLLATE utf8mb4_bin AS name
FROM _t0
ORDER BY
  1
