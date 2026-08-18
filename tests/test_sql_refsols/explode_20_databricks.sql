WITH _s1 AS (
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
FROM _s1 AS _s1, LATERAL POSEXPLODE(SPLIT(_s1.comment, '\\Q\\E')) AS _s0(idx, val)
WHERE
  _s0.val <> ''
GROUP BY
  1
