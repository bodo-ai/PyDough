WITH _t21 AS (
  SELECT
    s_key
  FROM main.sites
), _s0 AS (
  SELECT
    COUNT(*) OVER () AS n,
    CAST(1.0 AS REAL) / COUNT(*) OVER () AS page_rank,
    s_key
  FROM _t21
), _s1 AS (
  SELECT
    l_source,
    l_target
  FROM main.links
), _s2 AS (
  SELECT
    CAST(0.15 AS REAL) / MAX(_s0.n) AS damp_modifier,
    COALESCE(
      SUM(IIF(_s1.l_target IS NULL, _s0.n, CAST(_s1.l_source <> _s1.l_target AS INTEGER))),
      0
    ) AS n_out,
    MAX(_s0.page_rank) AS anything_page_rank,
    MAX(_s0.s_key) AS anything_s_key
  FROM _s0 AS _s0
  JOIN _s1 AS _s1
    ON _s0.s_key = _s1.l_source
  GROUP BY
    _s0.s_key
), _t16 AS (
  SELECT
    _s2.damp_modifier + 0.85 * SUM(
      CAST((
        CAST(_t22.l_source <> _t22.l_target OR _t22.l_target IS NULL AS INTEGER) * _s2.anything_page_rank
      ) AS REAL) / _s2.n_out
    ) OVER (PARTITION BY _s5.s_key) AS page_rank_0,
    _s2.damp_modifier,
    NOT _t22.l_target IS NULL AND _t22.l_source = _t22.l_target AS dummy_link,
    _s2.n_out,
    _s5.s_key
  FROM _s2 AS _s2
  JOIN _s1 AS _t22
    ON _s2.anything_s_key = _t22.l_source
  JOIN _t21 AS _s5
    ON _s5.s_key = _t22.l_target OR _t22.l_target IS NULL
), _t14 AS (
  SELECT
    _t16.damp_modifier + 0.85 * SUM(
      CAST((
        CAST(_t23.l_source <> _t23.l_target OR _t23.l_target IS NULL AS INTEGER) * _t16.page_rank_0
      ) AS REAL) / _t16.n_out
    ) OVER (PARTITION BY _s9.s_key) AS page_rank_0_2354,
    _t16.damp_modifier,
    NOT _t23.l_target IS NULL AND _t23.l_source = _t23.l_target AS dummy_link_2352,
    _t16.n_out,
    _s9.s_key
  FROM _t16 AS _t16
  JOIN _s1 AS _t23
    ON _t16.s_key = _t23.l_source
  JOIN _t21 AS _s9
    ON _s9.s_key = _t23.l_target OR _t23.l_target IS NULL
  WHERE
    _t16.dummy_link
), _t12 AS (
  SELECT
    _t14.damp_modifier + 0.85 * SUM(
      CAST((
        CAST(_t24.l_source <> _t24.l_target OR _t24.l_target IS NULL AS INTEGER) * _t14.page_rank_0_2354
      ) AS REAL) / _t14.n_out
    ) OVER (PARTITION BY _s13.s_key) AS page_rank_0_2364,
    _t14.damp_modifier,
    NOT _t24.l_target IS NULL AND _t24.l_source = _t24.l_target AS dummy_link_2362,
    _t14.n_out,
    _s13.s_key
  FROM _t14 AS _t14
  JOIN _s1 AS _t24
    ON _t14.s_key = _t24.l_source
  JOIN _t21 AS _s13
    ON _s13.s_key = _t24.l_target OR _t24.l_target IS NULL
  WHERE
    _t14.dummy_link_2352
), _t10 AS (
  SELECT
    _t12.damp_modifier + 0.85 * SUM(
      CAST((
        CAST(_t25.l_source <> _t25.l_target OR _t25.l_target IS NULL AS INTEGER) * _t12.page_rank_0_2364
      ) AS REAL) / _t12.n_out
    ) OVER (PARTITION BY _s17.s_key) AS page_rank_0_2374,
    _t12.damp_modifier,
    NOT _t25.l_target IS NULL AND _t25.l_source = _t25.l_target AS dummy_link_2372,
    _t12.n_out,
    _s17.s_key
  FROM _t12 AS _t12
  JOIN _s1 AS _t25
    ON _t12.s_key = _t25.l_source
  JOIN _t21 AS _s17
    ON _s17.s_key = _t25.l_target OR _t25.l_target IS NULL
  WHERE
    _t12.dummy_link_2362
), _t8 AS (
  SELECT
    _t10.damp_modifier + 0.85 * SUM(
      CAST((
        CAST(_t26.l_source <> _t26.l_target OR _t26.l_target IS NULL AS INTEGER) * _t10.page_rank_0_2374
      ) AS REAL) / _t10.n_out
    ) OVER (PARTITION BY _s21.s_key) AS page_rank_0_2384,
    _t10.damp_modifier,
    NOT _t26.l_target IS NULL AND _t26.l_source = _t26.l_target AS dummy_link_2382,
    _t10.n_out,
    _s21.s_key
  FROM _t10 AS _t10
  JOIN _s1 AS _t26
    ON _t10.s_key = _t26.l_source
  JOIN _t21 AS _s21
    ON _s21.s_key = _t26.l_target OR _t26.l_target IS NULL
  WHERE
    _t10.dummy_link_2372
), _t6 AS (
  SELECT
    _t8.damp_modifier + 0.85 * SUM(
      CAST((
        CAST(_t27.l_source <> _t27.l_target OR _t27.l_target IS NULL AS INTEGER) * _t8.page_rank_0_2384
      ) AS REAL) / _t8.n_out
    ) OVER (PARTITION BY _s25.s_key) AS page_rank_0_2394,
    _t8.damp_modifier,
    NOT _t27.l_target IS NULL AND _t27.l_source = _t27.l_target AS dummy_link_2392,
    _t8.n_out,
    _s25.s_key
  FROM _t8 AS _t8
  JOIN _s1 AS _t27
    ON _t27.l_source = _t8.s_key
  JOIN _t21 AS _s25
    ON _s25.s_key = _t27.l_target OR _t27.l_target IS NULL
  WHERE
    _t8.dummy_link_2382
), _t4 AS (
  SELECT
    _t6.damp_modifier + 0.85 * SUM(
      CAST((
        CAST(_t28.l_source <> _t28.l_target OR _t28.l_target IS NULL AS INTEGER) * _t6.page_rank_0_2394
      ) AS REAL) / _t6.n_out
    ) OVER (PARTITION BY _s29.s_key) AS page_rank_0_2404,
    _t6.damp_modifier,
    NOT _t28.l_target IS NULL AND _t28.l_source = _t28.l_target AS dummy_link_2402,
    _t6.n_out,
    _s29.s_key
  FROM _t6 AS _t6
  JOIN _s1 AS _t28
    ON _t28.l_source = _t6.s_key
  JOIN _t21 AS _s29
    ON _s29.s_key = _t28.l_target OR _t28.l_target IS NULL
  WHERE
    _t6.dummy_link_2392
), _t2 AS (
  SELECT
    _t4.damp_modifier + 0.85 * SUM(
      CAST((
        CAST(_t29.l_source <> _t29.l_target OR _t29.l_target IS NULL AS INTEGER) * _t4.page_rank_0_2404
      ) AS REAL) / _t4.n_out
    ) OVER (PARTITION BY _s33.s_key) AS page_rank_0_2414,
    NOT _t29.l_target IS NULL AND _t29.l_source = _t29.l_target AS dummy_link_2412,
    _s33.s_key
  FROM _t4 AS _t4
  JOIN _s1 AS _t29
    ON _t29.l_source = _t4.s_key
  JOIN _t21 AS _s33
    ON _s33.s_key = _t29.l_target OR _t29.l_target IS NULL
  WHERE
    _t4.dummy_link_2402
)
SELECT
  s_key AS key,
  ROUND(page_rank_0_2414, 5) AS page_rank
FROM _t2
WHERE
  dummy_link_2412
ORDER BY
  s_key
