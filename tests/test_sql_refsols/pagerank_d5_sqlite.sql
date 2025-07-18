WITH _t15 AS (
  SELECT
    s_key
  FROM main.sites
), _s0 AS (
  SELECT
    COUNT(*) OVER () AS n,
    CAST(1.0 AS REAL) / COUNT(*) OVER () AS page_rank,
    s_key
  FROM _t15
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
), _t10 AS (
  SELECT
    _s2.damp_modifier + 0.85 * SUM(
      CAST((
        CAST(_t16.l_source <> _t16.l_target OR _t16.l_target IS NULL AS INTEGER) * _s2.anything_page_rank
      ) AS REAL) / _s2.n_out
    ) OVER (PARTITION BY _s5.s_key) AS page_rank_0,
    _s2.damp_modifier,
    NOT _t16.l_target IS NULL AND _t16.l_source = _t16.l_target AS dummy_link,
    _s2.n_out,
    _s5.s_key
  FROM _s2 AS _s2
  JOIN _s1 AS _t16
    ON _s2.anything_s_key = _t16.l_source
  JOIN _t15 AS _s5
    ON _s5.s_key = _t16.l_target OR _t16.l_target IS NULL
), _t8 AS (
  SELECT
    _t10.damp_modifier + 0.85 * SUM(
      CAST((
        CAST(_t17.l_source <> _t17.l_target OR _t17.l_target IS NULL AS INTEGER) * _t10.page_rank_0
      ) AS REAL) / _t10.n_out
    ) OVER (PARTITION BY _s9.s_key) AS page_rank_0_256,
    _t10.damp_modifier,
    NOT _t17.l_target IS NULL AND _t17.l_source = _t17.l_target AS dummy_link_254,
    _t10.n_out,
    _s9.s_key
  FROM _t10 AS _t10
  JOIN _s1 AS _t17
    ON _t10.s_key = _t17.l_source
  JOIN _t15 AS _s9
    ON _s9.s_key = _t17.l_target OR _t17.l_target IS NULL
  WHERE
    _t10.dummy_link
), _t6 AS (
  SELECT
    _t8.damp_modifier + 0.85 * SUM(
      CAST((
        CAST(_t18.l_source <> _t18.l_target OR _t18.l_target IS NULL AS INTEGER) * _t8.page_rank_0_256
      ) AS REAL) / _t8.n_out
    ) OVER (PARTITION BY _s13.s_key) AS page_rank_0_266,
    _t8.damp_modifier,
    NOT _t18.l_target IS NULL AND _t18.l_source = _t18.l_target AS dummy_link_264,
    _t8.n_out,
    _s13.s_key
  FROM _t8 AS _t8
  JOIN _s1 AS _t18
    ON _t18.l_source = _t8.s_key
  JOIN _t15 AS _s13
    ON _s13.s_key = _t18.l_target OR _t18.l_target IS NULL
  WHERE
    _t8.dummy_link_254
), _t4 AS (
  SELECT
    _t6.damp_modifier + 0.85 * SUM(
      CAST((
        CAST(_t19.l_source <> _t19.l_target OR _t19.l_target IS NULL AS INTEGER) * _t6.page_rank_0_266
      ) AS REAL) / _t6.n_out
    ) OVER (PARTITION BY _s17.s_key) AS page_rank_0_276,
    _t6.damp_modifier,
    NOT _t19.l_target IS NULL AND _t19.l_source = _t19.l_target AS dummy_link_274,
    _t6.n_out,
    _s17.s_key
  FROM _t6 AS _t6
  JOIN _s1 AS _t19
    ON _t19.l_source = _t6.s_key
  JOIN _t15 AS _s17
    ON _s17.s_key = _t19.l_target OR _t19.l_target IS NULL
  WHERE
    _t6.dummy_link_264
), _t2 AS (
  SELECT
    _t4.damp_modifier + 0.85 * SUM(
      CAST((
        CAST(_t20.l_source <> _t20.l_target OR _t20.l_target IS NULL AS INTEGER) * _t4.page_rank_0_276
      ) AS REAL) / _t4.n_out
    ) OVER (PARTITION BY _s21.s_key) AS page_rank_0_286,
    NOT _t20.l_target IS NULL AND _t20.l_source = _t20.l_target AS dummy_link_284,
    _s21.s_key
  FROM _t4 AS _t4
  JOIN _s1 AS _t20
    ON _t20.l_source = _t4.s_key
  JOIN _t15 AS _s21
    ON _s21.s_key = _t20.l_target OR _t20.l_target IS NULL
  WHERE
    _t4.dummy_link_274
)
SELECT
  s_key AS key,
  ROUND(page_rank_0_286, 5) AS page_rank
FROM _t2
WHERE
  dummy_link_284
ORDER BY
  s_key
