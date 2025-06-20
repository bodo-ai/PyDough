SELECT
  _s0.sbcustid AS _id,
  _s0.sbcustname AS name
FROM main.sbcustomer AS _s0
JOIN main.sbtransaction AS _s1
  ON _s0.sbcustid = _s1.sbtxcustid
