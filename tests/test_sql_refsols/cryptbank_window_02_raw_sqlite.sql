WITH _t AS (
  SELECT
    accounts.a_key,
    accounts.a_open_ts,
    branches.b_name,
    ROW_NUMBER() OVER (PARTITION BY accounts.a_branchkey ORDER BY CAST(STRFTIME('%Y', DATETIME(accounts.a_open_ts, '+123456789 seconds')) AS INTEGER) = 2021, CASE
      WHEN accounts.a_key = 0
      THEN 0
      ELSE CASE WHEN accounts.a_key > 0 THEN 1 ELSE -1 END * CAST(SUBSTRING(
        accounts.a_key,
        1 + INSTR(accounts.a_key, '-'),
        CAST(LENGTH(accounts.a_key) AS REAL) / 2
      ) AS INTEGER)
    END) AS _w
  FROM crbnk.branches AS branches
  JOIN crbnk.accounts AS accounts
    ON accounts.a_branchkey = branches.b_key
  WHERE
    branches.b_addr LIKE '%;CA;%'
)
SELECT
  b_name AS branch_name,
  CASE
    WHEN a_key = 0
    THEN 0
    ELSE CASE WHEN a_key > 0 THEN 1 ELSE -1 END * CAST(SUBSTRING(a_key, 1 + INSTR(a_key, '-'), CAST(LENGTH(a_key) AS REAL) / 2) AS INTEGER)
  END AS key,
  DATETIME(a_open_ts, '+123456789 seconds') AS creation_timestamp
FROM _t
WHERE
  _w = 1
ORDER BY
  1
