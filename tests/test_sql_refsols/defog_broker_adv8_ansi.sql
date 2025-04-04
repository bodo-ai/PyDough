WITH _t2 AS (
  SELECT
    sbtxamount AS amount,
    sbtxcustid AS customer_id,
    sbtxdatetime AS date_time
  FROM main.sbtransaction
  WHERE
    sbtxdatetime < DATE_TRUNC('WEEK', CURRENT_TIMESTAMP())
    AND sbtxdatetime >= DATE_ADD(DATE_TRUNC('WEEK', CURRENT_TIMESTAMP()), -1, 'WEEK')
), _t0 AS (
  SELECT
    amount AS amount,
    customer_id AS customer_id
  FROM _t2
), _t3 AS (
  SELECT
    sbcustcountry AS country,
    sbcustid AS _id
  FROM main.sbcustomer
  WHERE
    LOWER(sbcustcountry) = 'usa'
), _t1 AS (
  SELECT
    _id AS _id
  FROM _t3
), _t1_2 AS (
  SELECT
    amount AS amount
  FROM _t0
  WHERE
    EXISTS(
      SELECT
        1 AS "1"
      FROM _t1
      WHERE
        customer_id = _id
    )
), _t0_2 AS (
  SELECT
    COUNT() AS agg_0,
    SUM(_t1.amount) AS agg_1
  FROM _t1_2 AS _t1
)
SELECT
  CASE WHEN _t0.agg_0 > 0 THEN _t0.agg_0 ELSE NULL END AS n_transactions,
  COALESCE(_t0.agg_1, 0) AS total_amount
FROM _t0_2 AS _t0
