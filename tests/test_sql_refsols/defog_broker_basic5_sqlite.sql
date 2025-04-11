WITH _t0 AS (
  SELECT
    sbtransaction.sbtxcustid AS customer_id,
    sbtransaction.sbtxtype AS transaction_type
  FROM main.sbtransaction AS sbtransaction
  WHERE
    sbtransaction.sbtxtype = 'buy'
), _s1 AS (
  SELECT
    _t0.customer_id AS customer_id
  FROM _t0 AS _t0
), _s0 AS (
  SELECT
    sbcustomer.sbcustid AS _id
  FROM main.sbcustomer AS sbcustomer
)
SELECT
  _s0._id AS _id
FROM _s0 AS _s0
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM _s1 AS _s1
    WHERE
      _s0._id = _s1.customer_id
  )
