WITH _t AS (
  SELECT
    CUSTOMER.c_acctbal,
    CUSTOMER.c_name,
    NATION.n_name,
    REGION.r_name,
    ROW_NUMBER() OVER (PARTITION BY NATION.n_regionkey ORDER BY CASE WHEN CUSTOMER.c_acctbal IS NULL THEN 1 ELSE 0 END DESC, CUSTOMER.c_acctbal DESC, CASE WHEN CUSTOMER.c_name COLLATE utf8mb4_bin IS NULL THEN 1 ELSE 0 END, CUSTOMER.c_name COLLATE utf8mb4_bin) AS _w
  FROM tpch.REGION AS REGION
  JOIN tpch.NATION AS NATION
    ON NATION.n_regionkey = REGION.r_regionkey
  JOIN tpch.CUSTOMER AS CUSTOMER
    ON CUSTOMER.c_nationkey = NATION.n_nationkey
)
SELECT
  r_name AS region_name,
  n_name AS nation_name,
  c_name AS customer_name,
  c_acctbal AS balance
FROM _t
WHERE
  _w = 1
