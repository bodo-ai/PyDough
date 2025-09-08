WITH _t12 AS (
  SELECT
    s_key
  FROM main.sites
), _s0 AS (
  SELECT
    s_key,
    COUNT(*) OVER () AS n,
    CAST(1.0 AS REAL) / COUNT(*) OVER () AS page_rank
  FROM _t12
), _s1 AS (
  SELECT
    l_source,
    l_target
  FROM main.links
), _s2 AS (
  SELECT
    _s0.s_key,
    MAX(_s0.n) AS anything_n,
    MAX(_s0.page_rank) AS anything_page_rank,
    SUM(IIF(_s1.l_target IS NULL, _s0.n, CAST(_s1.l_source <> _s1.l_target AS INTEGER))) AS sum_n_target
  FROM _s0 AS _s0
  JOIN _s1 AS _s1
    ON _s0.s_key = _s1.l_source
  GROUP BY
    1
), _t9 AS (
  SELECT
    _s2.anything_n,
    _s3.l_source,
    _s3.l_target,
    _s5.s_key,
    _s2.sum_n_target,
    (
      CAST(0.15 AS REAL) / _s2.anything_n
    ) + 0.85 * SUM(
      CAST((
        CAST(_s3.l_source <> _s3.l_target OR _s3.l_target IS NULL AS INTEGER) * _s2.anything_page_rank
      ) AS REAL) / COALESCE(_s2.sum_n_target, 0)
    ) OVER (PARTITION BY _s5.s_key) AS page_rank
  FROM _s2 AS _s2
  JOIN _s1 AS _s3
    ON _s2.s_key = _s3.l_source
  JOIN _t12 AS _s5
    ON _s3.l_target = _s5.s_key OR _s3.l_target IS NULL
), _t7 AS (
  SELECT
    _t9.anything_n,
    _s7.l_source,
    _s7.l_target,
    _s9.s_key,
    _t9.sum_n_target,
    (
      CAST(0.15 AS REAL) / _t9.anything_n
    ) + 0.85 * SUM(
      CAST((
        CAST(_s7.l_source <> _s7.l_target OR _s7.l_target IS NULL AS INTEGER) * _t9.page_rank
      ) AS REAL) / COALESCE(_t9.sum_n_target, 0)
    ) OVER (PARTITION BY _s9.s_key) AS page_rank
  FROM _t9 AS _t9
  JOIN _s1 AS _s7
    ON _s7.l_source = _t9.s_key
  JOIN _t12 AS _s9
    ON _s7.l_target = _s9.s_key OR _s7.l_target IS NULL
  WHERE
    NOT _t9.l_target IS NULL AND _t9.l_source = _t9.l_target
), _t5 AS (
  SELECT
    _t7.anything_n,
    _s11.l_source,
    _s11.l_target,
    _s13.s_key,
    _t7.sum_n_target,
    (
      CAST(0.15 AS REAL) / _t7.anything_n
    ) + 0.85 * SUM(
      CAST((
        CAST(_s11.l_source <> _s11.l_target OR _s11.l_target IS NULL AS INTEGER) * _t7.page_rank
      ) AS REAL) / COALESCE(_t7.sum_n_target, 0)
    ) OVER (PARTITION BY _s13.s_key) AS page_rank
  FROM _t7 AS _t7
  JOIN _s1 AS _s11
    ON _s11.l_source = _t7.s_key
  JOIN _t12 AS _s13
    ON _s11.l_target = _s13.s_key OR _s11.l_target IS NULL
  WHERE
    NOT _t7.l_target IS NULL AND _t7.l_source = _t7.l_target
), _t3 AS (
  SELECT
    _t5.anything_n,
    _s15.l_source,
    _s15.l_target,
    _s17.s_key,
    _t5.sum_n_target,
    (
      CAST(0.15 AS REAL) / _t5.anything_n
    ) + 0.85 * SUM(
      CAST((
        CAST(_s15.l_source <> _s15.l_target OR _s15.l_target IS NULL AS INTEGER) * _t5.page_rank
      ) AS REAL) / COALESCE(_t5.sum_n_target, 0)
    ) OVER (PARTITION BY _s17.s_key) AS page_rank
  FROM _t5 AS _t5
  JOIN _s1 AS _s15
    ON _s15.l_source = _t5.s_key
  JOIN _t12 AS _s17
    ON _s15.l_target = _s17.s_key OR _s15.l_target IS NULL
  WHERE
    NOT _t5.l_target IS NULL AND _t5.l_source = _t5.l_target
), _t1 AS (
  SELECT
    _s19.l_source,
    _s19.l_target,
    _s21.s_key,
    (
      CAST(0.15 AS REAL) / _t3.anything_n
    ) + 0.85 * SUM(
      CAST((
        CAST(_s19.l_source <> _s19.l_target OR _s19.l_target IS NULL AS INTEGER) * _t3.page_rank
      ) AS REAL) / COALESCE(_t3.sum_n_target, 0)
    ) OVER (PARTITION BY _s21.s_key) AS page_rank
  FROM _t3 AS _t3
  JOIN _s1 AS _s19
    ON _s19.l_source = _t3.s_key
  JOIN _t12 AS _s21
    ON _s19.l_target = _s21.s_key OR _s19.l_target IS NULL
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
