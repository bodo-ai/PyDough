WITH _t2 AS (
  SELECT
    sbtransaction.sbtxamount AS amount,
    sbtransaction.sbtxcustid AS customer_id,
    sbtransaction.sbtxdatetime AS date_time
  FROM main.sbtransaction AS sbtransaction
), _s0 AS (
  SELECT
    _t2.amount AS amount,
    _t2.customer_id AS customer_id
  FROM _t2 AS _t2
  WHERE
    _t2.date_time < DATE_TRUNC('WEEK', CURRENT_TIMESTAMP())
    AND _t2.date_time >= DATE_ADD(DATE_TRUNC('WEEK', CURRENT_TIMESTAMP()), -1, 'WEEK')
), _t3 AS (
  SELECT
    sbcustomer.sbcustid AS _id,
    sbcustomer.sbcustcountry AS country
  FROM main.sbcustomer AS sbcustomer
), _s1 AS (
  SELECT
    _t3._id AS _id
  FROM _t3 AS _t3
  WHERE
    LOWER(_t3.country) = 'usa'
), _t1 AS (
  SELECT
    _s0.amount AS amount
  FROM _s0 AS _s0
  WHERE
    EXISTS(
      SELECT
        1 AS "1"
      FROM _s1 AS _s1
      WHERE
        _s0.customer_id = _s1._id
    )
), _t0 AS (
  SELECT
    COUNT() AS agg_0,
    SUM(_t1.amount) AS agg_1
  FROM _t1 AS _t1
)
SELECT
  CASE WHEN _t0.agg_0 > 0 THEN _t0.agg_0 ELSE NULL END AS n_transactions,
  COALESCE(_t0.agg_1, 0) AS total_amount
FROM _t0 AS _t0
