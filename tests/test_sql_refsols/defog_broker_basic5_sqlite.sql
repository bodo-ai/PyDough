SELECT
  _s0.sbcustid AS _id
FROM main.sbcustomer AS _s0
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM main.sbtransaction AS _s1
    WHERE
      _s0.sbcustid = _s1.sbtxcustid AND _s1.sbtxtype = 'buy'
  )
