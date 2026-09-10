WITH _s1 AS (
  SELECT
    c_comment AS comment,
    c_custkey AS key
  FROM tpch.customer
  ORDER BY
    2
  LIMIT 3
)
SELECT
  _s1.key,
  _s0.idx AS idx1,
  _s2.idx AS idx2,
  _s2.val AS val2
FROM _s1 AS _s1
CROSS JOIN LATERAL POSEXPLODE(SPLIT(_s1.comment, '\\Q.\\E')) AS _s0(idx, val)
CROSS JOIN LATERAL POSEXPLODE(SPLIT(_s0.val, '\\Q,\\E')) AS _s2(idx, val)
ORDER BY
  1,
  2,
  3
