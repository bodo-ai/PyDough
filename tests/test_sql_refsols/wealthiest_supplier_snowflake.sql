WITH _t0 AS (
  SELECT
    s_acctbal,
    s_name
  FROM tpch.supplier
  QUALIFY
    ROW_NUMBER() OVER (ORDER BY s_acctbal DESC, s_name) = 1
)
SELECT
  s_name AS name,
  s_acctbal AS account_balance
FROM _t0
