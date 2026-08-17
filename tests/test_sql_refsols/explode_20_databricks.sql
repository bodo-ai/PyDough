WITH _q_0 AS (
  SELECT
    c_comment AS comment
  FROM tpch.customer
  ORDER BY
    c_custkey
  LIMIT 3
)
SELECT
  _s0.val AS char,
  COUNT(*) AS n
FROM _q_0 AS _q_0, LATERAL POSEXPLODE(SPLIT(_q_0.comment, '\\Q\\E')) AS _s0(idx, val)
WHERE
  _s0.val <> ''
GROUP BY
  1
