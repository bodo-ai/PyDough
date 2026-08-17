WITH _q_0 AS (
  SELECT
    c_comment AS comment,
    c_custkey AS key
  FROM tpch.customer
  ORDER BY
    2
  LIMIT 3
), _t0 AS (
  SELECT
    _s0.idx AS idx1,
    _s1.idx AS idx2,
    _s2.idx AS idx3,
    _q_0.key,
    _s2.val AS val3
  FROM _q_0 AS _q_0
  CROSS JOIN LATERAL POSEXPLODE(SPLIT(_q_0.comment, '\\Q.\\E')) AS _s0(idx, val)
  CROSS JOIN LATERAL POSEXPLODE(SPLIT(_s0.val, '\\Q,\\E')) AS _s1(idx, val), LATERAL POSEXPLODE(SPLIT(_s1.val, '\\Q \\E')) AS _s2(idx, val)
  WHERE
    _s2.val <> ''
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY _s0.idx, _q_0.key, _s1.idx ORDER BY _s2.idx DESC NULLS FIRST) = 1
)
SELECT
  key,
  idx1,
  idx2,
  idx3,
  val3
FROM _t0
