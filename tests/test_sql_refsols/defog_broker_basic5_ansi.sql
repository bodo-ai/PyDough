WITH _t0 AS (
  SELECT
    sbtxcustid AS customer_id,
    sbtxtype AS transaction_type
  FROM main.sbtransaction
  WHERE
    sbtxtype = 'buy'
), _t1 AS (
  SELECT
    customer_id AS customer_id
  FROM _t0
), _t0_2 AS (
  SELECT
    sbcustid AS _id
  FROM main.sbcustomer
)
SELECT
  _t0._id AS _id
FROM _t0_2 AS _t0
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM _t1
    WHERE
      _id = customer_id
  )
