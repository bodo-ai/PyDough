SELECT
  key AS id,
  ROW_NUMBER() OVER (ORDER BY acctbal DESC NULLS FIRST) AS rk
FROM (
  SELECT
    c_acctbal AS acctbal,
    c_custkey AS key
  FROM tpch.CUSTOMER
)
