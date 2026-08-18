WITH _s1 AS (
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
    _s2.idx AS idx2,
    _s4.idx AS idx3,
    _s1.key,
    _s4.val AS val3
  FROM _s1 AS _s1
  CROSS JOIN LATERAL POSEXPLODE(SPLIT(_s1.comment, '\\Q.\\E')) AS _s0(idx, val)
  CROSS JOIN LATERAL POSEXPLODE(SPLIT(_s0.val, '\\Q,\\E')) AS _s2(idx, val), LATERAL POSEXPLODE(SPLIT(_s2.val, '\\Q \\E')) AS _s4(idx, val)
  WHERE
    _s4.val <> ''
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY _s1.key ORDER BY _s0.idx DESC NULLS FIRST, _s2.idx NULLS LAST, _s4.idx NULLS LAST) = 1
)
SELECT
  key,
  idx1,
  idx2,
  idx3,
  val3
FROM _t0
