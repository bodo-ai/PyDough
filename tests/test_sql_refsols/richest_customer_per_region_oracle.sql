WITH _T AS (
  SELECT
    CUSTOMER.c_acctbal AS C_ACCTBAL,
    CUSTOMER.c_name AS C_NAME,
    NATION.n_name AS N_NAME,
    REGION.r_name AS R_NAME,
    ROW_NUMBER() OVER (PARTITION BY NATION.n_regionkey ORDER BY CUSTOMER.c_acctbal DESC, CUSTOMER.c_name) AS _W
  FROM TPCH.REGION REGION
  JOIN TPCH.NATION NATION
    ON NATION.n_regionkey = REGION.r_regionkey
  JOIN TPCH.CUSTOMER CUSTOMER
    ON CUSTOMER.c_nationkey = NATION.n_nationkey
)
SELECT
  R_NAME AS region_name,
  N_NAME AS nation_name,
  C_NAME AS customer_name,
  C_ACCTBAL AS balance
FROM _T
WHERE
  _W = 1
