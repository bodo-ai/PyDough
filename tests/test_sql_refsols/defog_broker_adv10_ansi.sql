WITH _s6 AS (
  SELECT
    COUNT(*) AS agg_0,
    _s1.sbcustid AS _id
  FROM main.sbcustomer AS _s1
  JOIN main.sbtransaction AS _s2
    ON EXTRACT(MONTH FROM _s1.sbcustjoindate) = EXTRACT(MONTH FROM _s2.sbtxdatetime)
    AND EXTRACT(YEAR FROM _s1.sbcustjoindate) = EXTRACT(YEAR FROM _s2.sbtxdatetime)
    AND _s1.sbcustid = _s2.sbtxcustid
  GROUP BY
    _s1.sbcustid
)
SELECT
  _s0.sbcustid AS _id,
  _s0.sbcustname AS name,
  COALESCE(_s6.agg_0, 0) AS num_transactions
FROM main.sbcustomer AS _s0
LEFT JOIN _s6 AS _s6
  ON _s0.sbcustid = _s6._id
ORDER BY
  num_transactions DESC
LIMIT 1
