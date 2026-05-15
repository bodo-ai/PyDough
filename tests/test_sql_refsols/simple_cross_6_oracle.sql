WITH "_T1" AS (
  SELECT
    c_acctbal AS C_ACCTBAL,
    c_custkey AS C_CUSTKEY,
    c_mktsegment AS C_MKTSEGMENT,
    c_nationkey AS C_NATIONKEY
  FROM TPCH.CUSTOMER
  WHERE
    c_acctbal > 9990
)
SELECT
  COUNT(*) AS n_pairs
FROM "_T1" "_T1"
JOIN "_T1" "_T2"
  ON "_T1".C_CUSTKEY < "_T2".C_CUSTKEY
  AND "_T1".C_MKTSEGMENT = "_T2".C_MKTSEGMENT
  AND "_T1".C_NATIONKEY = "_T2".C_NATIONKEY
