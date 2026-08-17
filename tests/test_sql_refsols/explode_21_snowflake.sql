WITH _q_0 AS (
  SELECT
    c_comment AS comment
  FROM tpch.customer
  ORDER BY
    c_custkey NULLS FIRST
  LIMIT 3
), _t0 AS (
  SELECT
    _s0.index - 1 AS idx1,
    _s1.index - 1 AS idx2,
    _s2.index - 1 AS idx3,
    _s3.index - 1 AS idx4,
    key,
    _s3.value AS val4
  FROM _q_0 AS _q_0
  CROSS JOIN LATERAL SPLIT_TO_TABLE(_q_0.comment, '.') AS _s0
  CROSS JOIN LATERAL SPLIT_TO_TABLE(_s0.value, ',') AS _s1
  CROSS JOIN LATERAL SPLIT_TO_TABLE(_s1.value, ' ') AS _s2, LATERAL FLATTEN(REGEXP_EXTRACT_ALL(_s2.value, '.{1}')) AS _s3(seq, key, path, index, value, this)
  WHERE
    _s3.value <> ''
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY _s1.index - 1, _s0.index - 1, key, _s2.index - 1 ORDER BY _s3.index - 1) = 1
)
SELECT
  key,
  idx1,
  idx2,
  idx3,
  idx4,
  val4
FROM _t0
