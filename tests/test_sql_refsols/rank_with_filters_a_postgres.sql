WITH _t0 AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY c_acctbal DESC, c_name) AS r,
    c_name
  FROM tpch.customer
)
SELECT
  c_name AS n,
  r
FROM _t0
WHERE
  c_name LIKE '%0' AND r <= 30
