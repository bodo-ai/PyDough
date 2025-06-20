SELECT
  _s0.sbcustid AS _id,
  _s0.sbcustname AS name
FROM main.sbcustomer AS _s0
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM main.sbtransaction AS _s1
    WHERE
      _s0.sbcustid = _s1.sbtxcustid
  )
