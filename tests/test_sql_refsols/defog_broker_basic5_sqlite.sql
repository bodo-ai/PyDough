WITH _t0 AS (
  SELECT
    sbtransaction.sbtxcustid AS sbtxcustid,
    sbtransaction.sbtxtype AS sbtxtype
  FROM main.sbtransaction AS sbtransaction
), _s1 AS (
  SELECT
    _t0.sbtxcustid AS sbtxcustid
  FROM _t0 AS _t0
  WHERE
    _t0.sbtxtype = 'buy'
), _s0 AS (
  SELECT
    sbcustomer.sbcustid AS sbcustid
  FROM main.sbcustomer AS sbcustomer
)
SELECT
  _s0.sbcustid AS _id
FROM _s0 AS _s0
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM _s1 AS _s1
    WHERE
      _s0.sbcustid = _s1.sbtxcustid
  )
