WITH _q_0 AS (
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
    _s1.idx AS idx2,
    _s2.idx AS idx3,
    _q_0.key,
    _s2.val AS val3
  FROM _q_0 AS _q_0
  CROSS JOIN LATERAL (
    SELECT
      UNNEST(STR_SPLIT(_q_0.comment, '.')) AS _col_0,
      GENERATE_SUBSCRIPTS(STR_SPLIT(_q_0.comment, '.'), 1) - 1 AS _col_1
  ) AS _s0(val, idx)
  CROSS JOIN LATERAL (
    SELECT
      UNNEST(STR_SPLIT(_s0.val, ',')) AS _col_0,
      GENERATE_SUBSCRIPTS(STR_SPLIT(_s0.val, ','), 1) - 1 AS _col_1
  ) AS _s1(val, idx)
  JOIN LATERAL (
    SELECT
      UNNEST(STR_SPLIT(_s1.val, ' ')) AS _col_0,
      GENERATE_SUBSCRIPTS(STR_SPLIT(_s1.val, ' '), 1) - 1 AS _col_1
  ) AS _s2(val, idx)
    ON _s2.val <> ''
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY _q_0.key ORDER BY _s0.idx, _s1.idx DESC NULLS FIRST, _s2.idx) = 1
)
SELECT
  key,
  idx1,
  idx2,
  idx3,
  val3
FROM _t0
