WITH _t AS (
  SELECT
    accounts.a_key,
    accounts.a_open_ts,
    branches.b_name,
    ROW_NUMBER() OVER (PARTITION BY accounts.a_branchkey ORDER BY accounts.a_open_ts IN ('2017-02-11 10:59:51', '2017-06-15 12:41:51', '2017-07-07 14:26:51', '2017-07-09 12:21:51', '2017-09-15 11:26:51', '2018-01-02 12:26:51'), CASE
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
