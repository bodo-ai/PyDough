WITH _s1 AS (
  SELECT
    COUNT(*) AS n_rows,
    SUM(sbtxamount) AS sum_sbtxamount,
    sbtxtickerid
  FROM main.sbtransaction
  GROUP BY
    sbtxtickerid
), _t0 AS (
  SELECT
    sbticker.sbtickersymbol AS sbtickersymbol_1,
    COALESCE(_s1.sum_sbtxamount, 0) AS total_amount_1,
    _s1.n_rows
  FROM main.sbticker AS sbticker
  LEFT JOIN _s1 AS _s1
    ON _s1.sbtxtickerid = sbticker.sbtickerid
  ORDER BY
    COALESCE(_s1.sum_sbtxamount, 0) DESC
  LIMIT 10
)
SELECT
  sbtickersymbol_1 AS symbol,
  COALESCE(n_rows, 0) AS num_transactions,
  total_amount_1 AS total_amount
FROM _t0
ORDER BY
  total_amount_1 DESC
