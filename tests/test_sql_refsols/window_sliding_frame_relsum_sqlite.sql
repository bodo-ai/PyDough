SELECT
  sbtxid AS transaction_id,
  SUM(sbtxshares) OVER (ORDER BY sbtxdatetime, sbtxid ROWS BETWEEN CURRENT ROW AND 4 FOLLOWING) AS w1,
  SUM(sbtxshares) OVER (PARTITION BY sbtxcustid ORDER BY sbtxdatetime, sbtxid ROWS BETWEEN CURRENT ROW AND 4 FOLLOWING) AS w2,
  SUM(sbtxshares) OVER (ORDER BY sbtxdatetime, sbtxid ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS w3,
  SUM(sbtxshares) OVER (PARTITION BY sbtxcustid ORDER BY sbtxdatetime, sbtxid ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS w4,
  SUM(sbtxshares) OVER (ORDER BY sbtxdatetime, sbtxid ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) AS w5,
  SUM(sbtxshares) OVER (PARTITION BY sbtxcustid ORDER BY sbtxdatetime, sbtxid ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) AS w6,
  SUM(sbtxshares) OVER (ORDER BY sbtxdatetime, sbtxid ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS w7,
  SUM(sbtxshares) OVER (PARTITION BY sbtxcustid ORDER BY sbtxdatetime, sbtxid ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS w8
FROM main.sbtransaction
ORDER BY
  sbtxdatetime
LIMIT 8
