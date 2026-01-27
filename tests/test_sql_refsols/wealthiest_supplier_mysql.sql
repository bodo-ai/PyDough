WITH _t AS (
  SELECT
    s_acctbal,
    s_name,
    ROW_NUMBER() OVER (ORDER BY CASE WHEN s_acctbal IS NULL THEN 1 ELSE 0 END DESC, s_acctbal DESC, CASE WHEN s_name COLLATE utf8mb4_bin IS NULL THEN 1 ELSE 0 END, s_name COLLATE utf8mb4_bin) AS _w
  FROM tpch.SUPPLIER
)
SELECT
  s_name AS name,
  s_acctbal AS account_balance
FROM _t
WHERE
  _w = 1
