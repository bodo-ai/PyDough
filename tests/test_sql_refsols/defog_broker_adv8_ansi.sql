WITH _t4 AS (
  SELECT
    sbtransaction.sbtxamount AS amount,
    sbtransaction.sbtxcustid AS customer_id,
    sbtransaction.sbtxdatetime AS date_time
  FROM main.sbtransaction AS sbtransaction
  WHERE
    sbtransaction.sbtxdatetime < DATE_TRUNC('WEEK', CURRENT_TIMESTAMP())
    AND sbtransaction.sbtxdatetime >= DATE_ADD(DATE_TRUNC('WEEK', CURRENT_TIMESTAMP()), -1, 'WEEK')
), _t3 AS (
  SELECT
    _t4.amount AS amount,
    _t4.customer_id AS customer_id
  FROM _t4 AS _t4
), _s0 AS (
  SELECT
    COUNT() AS agg_0,
    SUM(_t3.amount) AS agg_1,
    _t3.customer_id AS customer_id
  FROM _t3 AS _t3
  GROUP BY
    _t3.customer_id
), _t5 AS (
  SELECT
    sbcustomer.sbcustcountry AS country,
    sbcustomer.sbcustid AS _id
  FROM main.sbcustomer AS sbcustomer
  WHERE
    LOWER(sbcustomer.sbcustcountry) = 'usa'
), _s1 AS (
  SELECT
    _t5._id AS _id
  FROM _t5 AS _t5
), _t2 AS (
  SELECT
    _s0.agg_0 AS agg_0,
    _s0.agg_1 AS agg_1
  FROM _s0 AS _s0
  WHERE
    EXISTS(
      SELECT
        1 AS "1"
      FROM _s1 AS _s1
      WHERE
        _s0.customer_id = _s1._id
    )
), _t1 AS (
  SELECT
    SUM(_t2.agg_0) AS agg_0,
    SUM(_t2.agg_1) AS agg_1
  FROM _t2 AS _t2
), _t0 AS (
  SELECT
    COALESCE(_t1.agg_0, 0) AS agg_0,
    _t1.agg_1 AS agg_1
  FROM _t1 AS _t1
)
SELECT
  CASE WHEN _t0.agg_0 > 0 THEN _t0.agg_0 ELSE NULL END AS n_transactions,
  COALESCE(_t0.agg_1, 0) AS total_amount
FROM _t0 AS _t0
