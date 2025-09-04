WITH _t1 AS (
  SELECT
    SUM(sbtxstatus = 'success') AS agg_1,
    COUNT(*) AS n_rows,
    sbtxcustid
  FROM main.sbtransaction
  GROUP BY
    3
)
SELECT
  sbcustomer.sbcustname AS name,
  CAST((
    100.0 * COALESCE(_t1.agg_1, 0)
  ) AS REAL) / _t1.n_rows AS success_rate
FROM main.sbcustomer AS sbcustomer
JOIN _t1 AS _t1
  ON _t1.n_rows >= 5 AND _t1.sbtxcustid = sbcustomer.sbcustid
ORDER BY
  2
