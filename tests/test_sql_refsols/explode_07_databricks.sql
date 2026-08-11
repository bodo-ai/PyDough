WITH _q_0 AS (
  SELECT
    c_comment AS comment,
    c_custkey AS key
  FROM tpch.customer
  ORDER BY
    2
  LIMIT 3
)
SELECT
  _q_0.key,
  _s0.idx AS idx1,
  _s1.idx AS idx2,
  _s1.val AS val2
FROM _q_0 AS _q_0
CROSS JOIN LATERAL POSEXPLODE(SPLIT(_q_0.comment, '\\Q.\\E')) AS _s0(idx, val)
CROSS JOIN LATERAL POSEXPLODE(SPLIT(_s0.val, '\\Q,\\E')) AS _s1(idx, val)
ORDER BY
  1,
  2,
  3
