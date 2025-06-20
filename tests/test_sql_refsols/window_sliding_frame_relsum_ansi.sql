SELECT
  _s1.sbtxid AS transaction_id,
  SUM(_s1.sbtxshares) OVER (ORDER BY _s1.sbtxdatetime NULLS LAST, _s1.sbtxid NULLS LAST ROWS BETWEEN CURRENT ROW AND 4 FOLLOWING) AS w1,
  SUM(_s1.sbtxshares) OVER (PARTITION BY _s1.sbtxcustid ORDER BY _s1.sbtxdatetime NULLS LAST, _s1.sbtxid NULLS LAST ROWS BETWEEN CURRENT ROW AND 4 FOLLOWING) AS w2,
  SUM(_s1.sbtxshares) OVER (ORDER BY _s1.sbtxdatetime NULLS LAST, _s1.sbtxid NULLS LAST ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS w3,
  SUM(_s1.sbtxshares) OVER (PARTITION BY _s1.sbtxcustid ORDER BY _s1.sbtxdatetime NULLS LAST, _s1.sbtxid NULLS LAST ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS w4,
  SUM(_s1.sbtxshares) OVER (ORDER BY _s1.sbtxdatetime NULLS LAST, _s1.sbtxid NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) AS w5,
  SUM(_s1.sbtxshares) OVER (PARTITION BY _s1.sbtxcustid ORDER BY _s1.sbtxdatetime NULLS LAST, _s1.sbtxid NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) AS w6,
  SUM(_s1.sbtxshares) OVER (ORDER BY _s1.sbtxdatetime NULLS LAST, _s1.sbtxid NULLS LAST ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS w7,
  SUM(_s1.sbtxshares) OVER (PARTITION BY _s1.sbtxcustid ORDER BY _s1.sbtxdatetime NULLS LAST, _s1.sbtxid NULLS LAST ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS w8
FROM main.sbcustomer AS _s0
JOIN main.sbtransaction AS _s1
  ON _s0.sbcustid = _s1.sbtxcustid
ORDER BY
  _s1.sbtxdatetime
LIMIT 8
