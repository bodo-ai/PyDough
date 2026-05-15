WITH _t0 AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY CASE WHEN c_acctbal IS NULL THEN 1 ELSE 0 END DESC, c_acctbal DESC, CASE WHEN c_name COLLATE utf8mb4_bin IS NULL THEN 1 ELSE 0 END, c_name COLLATE utf8mb4_bin) AS r,
    c_name
  FROM tpch.CUSTOMER
)
SELECT
  c_name AS n,
  r
FROM _t0
WHERE
  c_name LIKE '%0' AND r <= 30
