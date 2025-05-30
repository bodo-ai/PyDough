WITH _t AS (
  SELECT
    s_acctbal AS account_balance,
    s_name AS name,
    ROW_NUMBER() OVER (ORDER BY s_acctbal DESC, s_name) AS _w
  FROM tpch.supplier
)
SELECT
  name,
  account_balance
FROM _t
WHERE
  _w = 1
