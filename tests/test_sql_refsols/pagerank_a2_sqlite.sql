WITH _t9 AS (
  SELECT
    s_key
  FROM main.sites
), _s0 AS (
  SELECT
    COUNT(*) OVER () AS n,
    CAST(1.0 AS REAL) / COUNT(*) OVER () AS page_rank,
    s_key
  FROM _t9
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
), _t4 AS (
  SELECT
    _s2.damp_modifier + 0.85 * SUM(
      CAST((
        CAST(_t10.l_source <> _t10.l_target OR _t10.l_target IS NULL AS INTEGER) * _s2.anything_page_rank
      ) AS REAL) / _s2.n_out
    ) OVER (PARTITION BY _s5.s_key) AS page_rank_0,
    _s2.damp_modifier,
    NOT _t10.l_target IS NULL AND _t10.l_source = _t10.l_target AS dummy_link,
    _s2.n_out,
    _s5.s_key
  FROM _s2 AS _s2
  JOIN _s1 AS _t10
    ON _s2.anything_s_key = _t10.l_source
  JOIN _t9 AS _s5
    ON _s5.s_key = _t10.l_target OR _t10.l_target IS NULL
), _t2 AS (
  SELECT
    _t4.damp_modifier + 0.85 * SUM(
      CAST((
        CAST(_t11.l_source <> _t11.l_target OR _t11.l_target IS NULL AS INTEGER) * _t4.page_rank_0
      ) AS REAL) / _t4.n_out
    ) OVER (PARTITION BY _s9.s_key) AS page_rank_0_20,
    NOT _t11.l_target IS NULL AND _t11.l_source = _t11.l_target AS dummy_link_18,
    _s9.s_key
  FROM _t4 AS _t4
  JOIN _s1 AS _t11
    ON _t11.l_source = _t4.s_key
  JOIN _t9 AS _s9
    ON _s9.s_key = _t11.l_target OR _t11.l_target IS NULL
  WHERE
    _t4.dummy_link
)
SELECT
  s_key AS key,
  ROUND(page_rank_0_20, 5) AS page_rank
FROM _t2
WHERE
  dummy_link_18
ORDER BY
  s_key
