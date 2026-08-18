WITH _s1 AS (
  SELECT
    c_comment AS comment,
    c_custkey AS key
  FROM tpch.customer
  ORDER BY
    2 NULLS FIRST
  LIMIT 3
), _t AS (
  SELECT
    _s0.idx - 1 AS idx1,
    _s2.idx - 1 AS idx2,
    _s4.idx - 1 AS idx3,
    _s1.key,
    _s4.val AS val3,
    ROW_NUMBER() OVER (PARTITION BY _s1.key ORDER BY _s0.idx - 1, _s2.idx - 1, _s4.idx - 1) AS _w
  FROM _s1 AS _s1
  CROSS JOIN UNNEST(SPLIT(_s1.comment, '.')) WITH ORDINALITY AS _s0(val, idx)
  CROSS JOIN UNNEST(SPLIT(_s0.val, ',')) WITH ORDINALITY AS _s2(val, idx), UNNEST(SPLIT(_s2.val, ' ')) WITH ORDINALITY AS _s4(val, idx)
  WHERE
    _s4.val <> ''
)
SELECT
  key,
  idx1,
  idx2,
  idx3,
  val3
FROM _t
WHERE
  _w = 1
