WITH _t8 AS (
  SELECT
    s_key
  FROM main.sites
), _s0 AS (
  SELECT
    COUNT(*) OVER () AS n,
    CAST(1.0 AS REAL) / COUNT(*) OVER () AS page_rank,
    s_key
  FROM _t8
), _s1 AS (
  SELECT
    l_source,
    l_target
  FROM main.links
), _s2 AS (
  SELECT
    MAX(_s0.n) AS anything_n,
    MAX(_s0.page_rank) AS anything_page_rank,
    SUM(IIF(_s1.l_target IS NULL, _s0.n, CAST(_s1.l_source <> _s1.l_target AS INTEGER))) AS sum_n_target,
    _s0.s_key
  FROM _s0 AS _s0
  JOIN _s1 AS _s1
    ON _s0.s_key = _s1.l_source
  GROUP BY
    4
), _t5 AS (
  SELECT
    (
      CAST(0.15 AS REAL) / _s2.anything_n
    ) + 0.85 * SUM(
      CAST((
        CAST(_s3.l_source <> _s3.l_target OR _s3.l_target IS NULL AS INTEGER) * _s2.anything_page_rank
      ) AS REAL) / COALESCE(_s2.sum_n_target, 0)
    ) OVER (PARTITION BY _s5.s_key) AS page_rank,
    _s2.anything_n,
    _s3.l_source,
    _s3.l_target,
    _s5.s_key,
    _s2.sum_n_target
  FROM _s2 AS _s2
  JOIN _s1 AS _s3
    ON _s2.s_key = _s3.l_source
  JOIN _t8 AS _s5
    ON _s3.l_target = _s5.s_key OR _s3.l_target IS NULL
), _t3 AS (
  SELECT
    (
      CAST(0.15 AS REAL) / _t5.anything_n
    ) + 0.85 * SUM(
      CAST((
        CAST(_s7.l_source <> _s7.l_target OR _s7.l_target IS NULL AS INTEGER) * _t5.page_rank
      ) AS REAL) / COALESCE(_t5.sum_n_target, 0)
    ) OVER (PARTITION BY _s9.s_key) AS page_rank,
    _t5.anything_n,
    _s7.l_source,
    _s7.l_target,
    _s9.s_key,
    _t5.sum_n_target
  FROM _t5 AS _t5
  JOIN _s1 AS _s7
    ON _s7.l_source = _t5.s_key
  JOIN _t8 AS _s9
    ON _s7.l_target = _s9.s_key OR _s7.l_target IS NULL
  WHERE
    NOT _t5.l_target IS NULL AND _t5.l_source = _t5.l_target
), _t1 AS (
  SELECT
    (
      CAST(0.15 AS REAL) / _t3.anything_n
    ) + 0.85 * SUM(
      CAST((
        CAST(_s11.l_source <> _s11.l_target OR _s11.l_target IS NULL AS INTEGER) * _t3.page_rank
      ) AS REAL) / COALESCE(_t3.sum_n_target, 0)
    ) OVER (PARTITION BY _s13.s_key) AS page_rank,
    _s11.l_source,
    _s11.l_target,
    _s13.s_key
  FROM _t3 AS _t3
  JOIN _s1 AS _s11
    ON _s11.l_source = _t3.s_key
  JOIN _t8 AS _s13
    ON _s11.l_target = _s13.s_key OR _s11.l_target IS NULL
  WHERE
    NOT _t3.l_target IS NULL AND _t3.l_source = _t3.l_target
)
SELECT
  s_key AS key,
  ROUND(page_rank, 5) AS page_rank
FROM _t1
WHERE
  NOT l_target IS NULL AND l_source = l_target
ORDER BY
  1
