WITH _s1 AS (
  SELECT
    sbtxcustid,
    sbtxdatetime
  FROM main.sbtransaction
), _t0 AS (
  SELECT
    _s1.sbtxcustid,
    ANY_VALUE(sbcustomer.sbcustname) AS anything_sbcustname,
    COUNT(*) AS n_rows
  FROM main.sbcustomer AS sbcustomer
  LEFT JOIN _s1 AS _s1
    ON EXTRACT(MONTH FROM CAST(_s1.sbtxdatetime AS DATETIME)) = EXTRACT(MONTH FROM CAST(sbcustomer.sbcustjoindate AS DATETIME))
    AND EXTRACT(YEAR FROM CAST(_s1.sbtxdatetime AS DATETIME)) = EXTRACT(YEAR FROM CAST(sbcustomer.sbcustjoindate AS DATETIME))
    AND _s1.sbtxcustid = sbcustomer.sbcustid
  GROUP BY
    EXTRACT(MONTH FROM CAST(_s1.sbtxdatetime AS DATETIME)),
    EXTRACT(YEAR FROM CAST(_s1.sbtxdatetime AS DATETIME)),
    1
)
SELECT
  sbtxcustid AS _id,
  anything_sbcustname AS name,
  n_rows * CASE WHEN NOT sbtxcustid IS NULL THEN 1 ELSE 0 END AS num_transactions
FROM _t0
ORDER BY
  3 DESC
LIMIT 1
