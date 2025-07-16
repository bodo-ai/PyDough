WITH _t11 AS (
  SELECT
    s_key
  FROM main.sites
), _s0 AS (
  SELECT
    COUNT(*) OVER () AS n,
    s_key
  FROM _t11
), _s1 AS (
  SELECT
    l_source,
    l_target
  FROM main.links
), _s2 AS (
  SELECT
    COALESCE(
      SUM(IIF(_s1.l_target IS NULL, _s0.n, CAST(_s1.l_source <> _s1.l_target AS INTEGER))),
      0
    ) AS n_out,
    CAST(1.0 AS REAL) / MAX(_s0.n) AS page_rank,
    MAX(_s0.n) AS anything_n,
    MAX(_s0.s_key) AS anything_s_key
  FROM _s0 AS _s0
  JOIN _s1 AS _s1
    ON _s0.s_key = _s1.l_source
  GROUP BY
    _s0.s_key
), _t6 AS (
  SELECT
    (
      CAST(0.15000000000000002 AS REAL) / _s2.anything_n
    ) + 0.85 * SUM(
      CAST((
        CAST(_t12.l_source <> _t12.l_target OR _t12.l_target IS NULL AS INTEGER) * _s2.page_rank
      ) AS REAL) / _s2.n_out
    ) OVER (PARTITION BY _s5.s_key) AS page_rank_0,
    _s2.anything_n,
    NOT _t12.l_target IS NULL AND _t12.l_source = _t12.l_target AS dummy_link,
    _s2.n_out,
    _s5.s_key
  FROM _s2 AS _s2
  JOIN _s1 AS _t12
    ON _s2.anything_s_key = _t12.l_source
  JOIN _t11 AS _s5
    ON _s5.s_key = _t12.l_target OR _t12.l_target IS NULL
), _t4 AS (
  SELECT
    (
      CAST(0.15000000000000002 AS REAL) / _t6.anything_n
    ) + 0.85 * SUM(
      CAST((
        CAST(_t13.l_source <> _t13.l_target OR _t13.l_target IS NULL AS INTEGER) * _t6.page_rank_0
      ) AS REAL) / _t6.n_out
    ) OVER (PARTITION BY _s9.s_key) AS page_rank_0_48,
    _t6.anything_n,
    NOT _t13.l_target IS NULL AND _t13.l_source = _t13.l_target AS dummy_link_46,
    _t6.n_out,
    _s9.s_key
  FROM _t6 AS _t6
  JOIN _s1 AS _t13
    ON _t13.l_source = _t6.s_key
  JOIN _t11 AS _s9
    ON _s9.s_key = _t13.l_target OR _t13.l_target IS NULL
  WHERE
    _t6.dummy_link
), _t2 AS (
  SELECT
    (
      CAST(0.15000000000000002 AS REAL) / _t4.anything_n
    ) + 0.85 * SUM(
      CAST((
        CAST(_t14.l_source <> _t14.l_target OR _t14.l_target IS NULL AS INTEGER) * _t4.page_rank_0_48
      ) AS REAL) / _t4.n_out
    ) OVER (PARTITION BY _s13.s_key) AS page_rank_0_58,
    NOT _t14.l_target IS NULL AND _t14.l_source = _t14.l_target AS dummy_link_56,
    _s13.s_key
  FROM _t4 AS _t4
  JOIN _s1 AS _t14
    ON _t14.l_source = _t4.s_key
  JOIN _t11 AS _s13
    ON _s13.s_key = _t14.l_target OR _t14.l_target IS NULL
  WHERE
    _t4.dummy_link_46
)
SELECT
  s_key AS key,
  ROUND(page_rank_0_58, 5) AS page_rank
FROM _t2
WHERE
  dummy_link_56
ORDER BY
  s_key
