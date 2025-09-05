WITH _t18 AS (
  SELECT
    s_key
  FROM main.sites
), _s0 AS (
  SELECT
    COUNT(*) OVER () AS n,
    CAST(1.0 AS REAL) / COUNT(*) OVER () AS page_rank,
    s_key
  FROM _t18
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
), _t15 AS (
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
  JOIN _t18 AS _s5
    ON _s3.l_target = _s5.s_key OR _s3.l_target IS NULL
), _t13 AS (
  SELECT
    (
      CAST(0.15 AS REAL) / _t15.anything_n
    ) + 0.85 * SUM(
      CAST((
        CAST(_s7.l_source <> _s7.l_target OR _s7.l_target IS NULL AS INTEGER) * _t15.page_rank
      ) AS REAL) / COALESCE(_t15.sum_n_target, 0)
    ) OVER (PARTITION BY _s9.s_key) AS page_rank,
    _t15.anything_n,
    _s7.l_source,
    _s7.l_target,
    _s9.s_key,
    _t15.sum_n_target
  FROM _t15 AS _t15
  JOIN _s1 AS _s7
    ON _s7.l_source = _t15.s_key
  JOIN _t18 AS _s9
    ON _s7.l_target = _s9.s_key OR _s7.l_target IS NULL
  WHERE
    NOT _t15.l_target IS NULL AND _t15.l_source = _t15.l_target
), _t11 AS (
  SELECT
    (
      CAST(0.15 AS REAL) / _t13.anything_n
    ) + 0.85 * SUM(
      CAST((
        CAST(_s11.l_source <> _s11.l_target OR _s11.l_target IS NULL AS INTEGER) * _t13.page_rank
      ) AS REAL) / COALESCE(_t13.sum_n_target, 0)
    ) OVER (PARTITION BY _s13.s_key) AS page_rank,
    _t13.anything_n,
    _s11.l_source,
    _s11.l_target,
    _s13.s_key,
    _t13.sum_n_target
  FROM _t13 AS _t13
  JOIN _s1 AS _s11
    ON _s11.l_source = _t13.s_key
  JOIN _t18 AS _s13
    ON _s11.l_target = _s13.s_key OR _s11.l_target IS NULL
  WHERE
    NOT _t13.l_target IS NULL AND _t13.l_source = _t13.l_target
), _t9 AS (
  SELECT
    (
      CAST(0.15 AS REAL) / _t11.anything_n
    ) + 0.85 * SUM(
      CAST((
        CAST(_s15.l_source <> _s15.l_target OR _s15.l_target IS NULL AS INTEGER) * _t11.page_rank
      ) AS REAL) / COALESCE(_t11.sum_n_target, 0)
    ) OVER (PARTITION BY _s17.s_key) AS page_rank,
    _t11.anything_n,
    _s15.l_source,
    _s15.l_target,
    _s17.s_key,
    _t11.sum_n_target
  FROM _t11 AS _t11
  JOIN _s1 AS _s15
    ON _s15.l_source = _t11.s_key
  JOIN _t18 AS _s17
    ON _s15.l_target = _s17.s_key OR _s15.l_target IS NULL
  WHERE
    NOT _t11.l_target IS NULL AND _t11.l_source = _t11.l_target
), _t7 AS (
  SELECT
    (
      CAST(0.15 AS REAL) / _t9.anything_n
    ) + 0.85 * SUM(
      CAST((
        CAST(_s19.l_source <> _s19.l_target OR _s19.l_target IS NULL AS INTEGER) * _t9.page_rank
      ) AS REAL) / COALESCE(_t9.sum_n_target, 0)
    ) OVER (PARTITION BY _s21.s_key) AS page_rank,
    _t9.anything_n,
    _s19.l_source,
    _s19.l_target,
    _s21.s_key,
    _t9.sum_n_target
  FROM _t9 AS _t9
  JOIN _s1 AS _s19
    ON _s19.l_source = _t9.s_key
  JOIN _t18 AS _s21
    ON _s19.l_target = _s21.s_key OR _s19.l_target IS NULL
  WHERE
    NOT _t9.l_target IS NULL AND _t9.l_source = _t9.l_target
), _t5 AS (
  SELECT
    (
      CAST(0.15 AS REAL) / _t7.anything_n
    ) + 0.85 * SUM(
      CAST((
        CAST(_s23.l_source <> _s23.l_target OR _s23.l_target IS NULL AS INTEGER) * _t7.page_rank
      ) AS REAL) / COALESCE(_t7.sum_n_target, 0)
    ) OVER (PARTITION BY _s25.s_key) AS page_rank,
    _t7.anything_n,
    _s23.l_source,
    _s23.l_target,
    _s25.s_key,
    _t7.sum_n_target
  FROM _t7 AS _t7
  JOIN _s1 AS _s23
    ON _s23.l_source = _t7.s_key
  JOIN _t18 AS _s25
    ON _s23.l_target = _s25.s_key OR _s23.l_target IS NULL
  WHERE
    NOT _t7.l_target IS NULL AND _t7.l_source = _t7.l_target
), _t3 AS (
  SELECT
    (
      CAST(0.15 AS REAL) / _t5.anything_n
    ) + 0.85 * SUM(
      CAST((
        CAST(_s27.l_source <> _s27.l_target OR _s27.l_target IS NULL AS INTEGER) * _t5.page_rank
      ) AS REAL) / COALESCE(_t5.sum_n_target, 0)
    ) OVER (PARTITION BY _s29.s_key) AS page_rank,
    _t5.anything_n,
    _s27.l_source,
    _s27.l_target,
    _s29.s_key,
    _t5.sum_n_target
  FROM _t5 AS _t5
  JOIN _s1 AS _s27
    ON _s27.l_source = _t5.s_key
  JOIN _t18 AS _s29
    ON _s27.l_target = _s29.s_key OR _s27.l_target IS NULL
  WHERE
    NOT _t5.l_target IS NULL AND _t5.l_source = _t5.l_target
), _t1 AS (
  SELECT
    (
      CAST(0.15 AS REAL) / _t3.anything_n
    ) + 0.85 * SUM(
      CAST((
        CAST(_s31.l_source <> _s31.l_target OR _s31.l_target IS NULL AS INTEGER) * _t3.page_rank
      ) AS REAL) / COALESCE(_t3.sum_n_target, 0)
    ) OVER (PARTITION BY _s33.s_key) AS page_rank,
    _s31.l_source,
    _s31.l_target,
    _s33.s_key
  FROM _t3 AS _t3
  JOIN _s1 AS _s31
    ON _s31.l_source = _t3.s_key
  JOIN _t18 AS _s33
    ON _s31.l_target = _s33.s_key OR _s31.l_target IS NULL
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
