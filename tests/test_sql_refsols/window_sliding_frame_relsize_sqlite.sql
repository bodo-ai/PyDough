SELECT
  _s1.sbtxid AS transaction_id,
  COUNT(*) OVER (ORDER BY _s1.sbtxdatetime, _s1.sbtxid ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS w1,
  COUNT(*) OVER (PARTITION BY _s1.sbtxcustid ORDER BY _s1.sbtxdatetime, _s1.sbtxid ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS w2,
  COUNT(*) OVER (ORDER BY _s1.sbtxdatetime, _s1.sbtxid ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS w3,
  COUNT(*) OVER (PARTITION BY _s1.sbtxcustid ORDER BY _s1.sbtxdatetime, _s1.sbtxid ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS w4,
  COUNT(*) OVER (ORDER BY _s1.sbtxdatetime, _s1.sbtxid ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS w5,
  COUNT(*) OVER (PARTITION BY _s1.sbtxcustid ORDER BY _s1.sbtxdatetime, _s1.sbtxid ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS w6,
  COUNT(*) OVER (ORDER BY _s1.sbtxdatetime, _s1.sbtxid ROWS BETWEEN 3 PRECEDING AND 5 FOLLOWING) AS w7,
  COUNT(*) OVER (PARTITION BY _s1.sbtxcustid ORDER BY _s1.sbtxdatetime, _s1.sbtxid ROWS BETWEEN 3 PRECEDING AND 5 FOLLOWING) AS w8
FROM main.sbcustomer AS _s0
JOIN main.sbtransaction AS _s1
  ON _s0.sbcustid = _s1.sbtxcustid
ORDER BY
  _s1.sbtxdatetime
LIMIT 8
