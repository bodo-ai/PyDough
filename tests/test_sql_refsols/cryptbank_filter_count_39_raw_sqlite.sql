WITH _t1 AS (
  SELECT
    a_open_ts
  FROM crbnk.accounts
  WHERE
    MIN(
      CAST(STRFTIME('%H', DATETIME(a_open_ts, '+123456789 seconds')) AS INTEGER),
      CAST(STRFTIME('%M', DATETIME(a_open_ts, '+123456789 seconds')) AS INTEGER)
    ) = 15
)
SELECT
  COUNT(*) AS n
FROM _t1
