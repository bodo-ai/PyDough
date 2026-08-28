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
    _s0.index - 1 AS idx1,
    _s2.index - 1 AS idx2,
    _s4.index - 1 AS idx3,
    _s6.index AS idx4,
    _s1.key,
    _s6.value AS val4
  FROM _s1 AS _s1
  CROSS JOIN LATERAL SPLIT_TO_TABLE(_s1.comment, '.') AS _s0
  CROSS JOIN LATERAL SPLIT_TO_TABLE(_s0.value, ',') AS _s2
  CROSS JOIN LATERAL SPLIT_TO_TABLE(_s2.value, ' ') AS _s4, LATERAL FLATTEN(REGEXP_EXTRACT_ALL(_s4.value, '.{1}')) AS _s6(seq, key, path, index, value, this)
  WHERE
    _s6.value <> ''
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY _s2.index - 1, _s0.index - 1, _s1.key, _s4.index - 1 ORDER BY _s6.index) = 1
)
SELECT
  key,
  idx1,
  idx2,
  idx3,
  idx4,
  val4
FROM _t0
