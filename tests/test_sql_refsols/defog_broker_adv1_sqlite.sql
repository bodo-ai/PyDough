WITH _t1 AS (
  SELECT
    SUM(sbtxamount) AS agg_0,
    sbtxcustid AS customer_id
  FROM main.sbtransaction
  GROUP BY
    sbtxcustid
), _t0_2 AS (
  SELECT
    sbcustomer.sbcustname AS name,
    COALESCE(_t1.agg_0, 0) AS ordering_1,
    COALESCE(_t1.agg_0, 0) AS total_amount
  FROM main.sbcustomer AS sbcustomer
  LEFT JOIN _t1 AS _t1
    ON _t1.customer_id = sbcustomer.sbcustid
  ORDER BY
    ordering_1 DESC
  LIMIT 5
)
SELECT
  name,
  total_amount
FROM _t0_2
ORDER BY
  ordering_1 DESC
