WITH _t0 AS (
  SELECT
    s_acctbal AS account_balance,
    s_name AS name
  FROM tpch.supplier
  QUALIFY
    ROW_NUMBER() OVER (ORDER BY s_acctbal DESC NULLS FIRST, s_name NULLS LAST) = 1
)
SELECT
  name,
  account_balance
FROM _t0
