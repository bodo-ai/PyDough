SELECT
  ROW_NUMBER() OVER (ORDER BY acctbal DESC) AS rank
FROM (
  SELECT
    c_acctbal AS acctbal
  FROM tpch.CUSTOMER
)
