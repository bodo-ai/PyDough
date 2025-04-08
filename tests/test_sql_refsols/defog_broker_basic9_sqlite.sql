WITH _s1 AS (
  SELECT
    sbtransaction.sbtxcustid AS customer_id
  FROM main.sbtransaction AS sbtransaction
), _s0 AS (
  SELECT
    sbcustomer.sbcustid AS _id,
    sbcustomer.sbcustname AS name
  FROM main.sbcustomer AS sbcustomer
)
SELECT
  _s0._id AS _id,
  _s0.name AS name
FROM _s0 AS _s0
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM _s1 AS _s1
    WHERE
      _s0._id = _s1.customer_id
  )
