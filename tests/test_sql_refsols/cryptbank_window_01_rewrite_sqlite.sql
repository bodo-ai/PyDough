WITH _t AS (
  SELECT
    a_balance,
    a_key,
    a_type,
    ROW_NUMBER() OVER (PARTITION BY a_type IN ('avingss', 'etirementr') ORDER BY SQRT(a_balance) DESC) AS _w
  FROM crbnk.accounts
)
SELECT
  SUBSTRING(a_type, -1) || SUBSTRING(a_type, 1, LENGTH(a_type) - 1) AS account_type,
  CASE
    WHEN a_key = 0
    THEN 0
    ELSE CASE WHEN a_key > 0 THEN 1 ELSE -1 END * CAST(SUBSTRING(a_key, 1 + INSTR(a_key, '-'), CAST(LENGTH(a_key) AS REAL) / 2) AS INTEGER)
  END AS key,
  SQRT(a_balance) AS balance
FROM _t
WHERE
  _w = 1
ORDER BY
  1
