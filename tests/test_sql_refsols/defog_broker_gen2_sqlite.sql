SELECT
  COUNT(_s0.sbtxcustid) AS transaction_count
FROM main.sbtransaction AS _s0
JOIN main.sbcustomer AS _s1
  ON _s0.sbtxcustid = _s1.sbcustid
  AND _s1.sbcustjoindate >= DATETIME('now', '-70 day')
