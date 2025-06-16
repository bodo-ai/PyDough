WITH _s1 AS (
  SELECT
    sbtransaction.sbtxcustid AS sbtxcustid
  FROM main.sbtransaction AS sbtransaction
), _s0 AS (
  SELECT
    sbcustomer.sbcustid AS sbcustid,
    sbcustomer.sbcustname AS sbcustname
  FROM main.sbcustomer AS sbcustomer
)
SELECT
  _s0.sbcustid AS _id,
  _s0.sbcustname AS name
FROM _s0 AS _s0
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM _s1 AS _s1
    WHERE
      _s0.sbcustid = _s1.sbtxcustid
  )
