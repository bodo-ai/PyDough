WITH _t0 AS (
  SELECT
    c_name AS n,
    ROW_NUMBER() OVER (ORDER BY c_acctbal DESC NULLS FIRST) AS r,
    c_name AS name
  FROM tpch.customer
)
SELECT
  n,
  r
FROM _t0
WHERE
  name LIKE '%0' AND r <= 30
