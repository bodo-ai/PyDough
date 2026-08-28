WITH _s1 AS (
  SELECT
    c_comment AS comment,
    c_custkey AS key
  FROM tpch.customer
  ORDER BY
    2 NULLS FIRST
  LIMIT 3
), _t0 AS (
  SELECT
    _s0.idx AS idx1,
    _s2.idx AS idx2,
    _s4.idx AS idx3,
    _s1.key,
    _s4.val AS val3
  FROM _s1 AS _s1
  CROSS JOIN LATERAL (
    SELECT
      UNNEST(STR_SPLIT(_s1.comment, '.')) AS _col_0,
      GENERATE_SUBSCRIPTS(STR_SPLIT(_s1.comment, '.'), 1) - 1 AS _col_1
  ) AS _s0(val, idx)
  CROSS JOIN LATERAL (
    SELECT
      UNNEST(STR_SPLIT(_s0.val, ',')) AS _col_0,
      GENERATE_SUBSCRIPTS(STR_SPLIT(_s0.val, ','), 1) - 1 AS _col_1
  ) AS _s2(val, idx), LATERAL (
    SELECT
      UNNEST(STR_SPLIT(_s2.val, ' ')) AS _col_0,
      GENERATE_SUBSCRIPTS(STR_SPLIT(_s2.val, ' '), 1) - 1 AS _col_1
  ) AS _s4(val, idx)
  WHERE
    _s4.val <> ''
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY _s1.key ORDER BY _s0.idx DESC NULLS FIRST, _s2.idx, _s4.idx) = 1
)
SELECT
  key,
  idx1,
  idx2,
  idx3,
  val3
FROM _t0
