WITH _s1 AS (
  SELECT
    sbtxtickerid,
    COUNT(*) AS n_rows,
    SUM(sbtxamount) AS sum_sbtxamount
  FROM main.sbtransaction
  GROUP BY
    1
)
SELECT
  sbticker.sbtickersymbol AS symbol,
  COALESCE(_s1.n_rows, 0) AS num_transactions,
  COALESCE(_s1.sum_sbtxamount, 0) AS total_amount
FROM main.sbticker AS sbticker
LEFT JOIN _s1 AS _s1
  ON _s1.sbtxtickerid = sbticker.sbtickerid
ORDER BY
  3 DESC NULLS LAST
LIMIT 10
