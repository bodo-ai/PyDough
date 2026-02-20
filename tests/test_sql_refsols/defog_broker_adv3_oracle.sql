WITH "_T1" AS (
  SELECT
    sbtxcustid AS SBTXCUSTID,
    COUNT(*) AS N_ROWS,
    SUM(sbtxstatus = 'success') AS SUM_EXPR
  FROM MAIN.SBTRANSACTION
  GROUP BY
    sbtxcustid
)
SELECT
  SBCUSTOMER.sbcustname AS name,
  (
    100.0 * COALESCE("_T1".SUM_EXPR, 0)
  ) / "_T1".N_ROWS AS success_rate
FROM MAIN.SBCUSTOMER SBCUSTOMER
JOIN "_T1" "_T1"
  ON SBCUSTOMER.sbcustid = "_T1".SBTXCUSTID AND "_T1".N_ROWS >= 5
ORDER BY
  2 NULLS FIRST
