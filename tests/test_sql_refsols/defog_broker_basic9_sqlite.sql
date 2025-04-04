WITH _t1 AS (
  SELECT
    sbtxcustid AS customer_id
  FROM main.sbtransaction
), _t0 AS (
  SELECT
    sbcustid AS _id,
    sbcustname AS name
  FROM main.sbcustomer
)
SELECT
  _id AS _id,
  name AS name
FROM _t0
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM _t1
    WHERE
      _id = customer_id
  )
