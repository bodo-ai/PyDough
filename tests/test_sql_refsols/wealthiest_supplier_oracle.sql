WITH "_T" AS (
  SELECT
    s_acctbal AS S_ACCTBAL,
    s_name AS S_NAME,
    ROW_NUMBER() OVER (ORDER BY s_acctbal DESC, s_name) AS "_W"
  FROM TPCH.SUPPLIER
)
SELECT
  S_NAME AS name,
  S_ACCTBAL AS account_balance
FROM "_T"
WHERE
  "_W" = 1
