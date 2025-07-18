WITH _t7 AS (
  SELECT
    s_key
  FROM main.sites
), _s0 AS (
  SELECT
    COUNT(*) OVER () AS n,
    CAST(1.0 AS REAL) / COUNT(*) OVER () AS page_rank,
    s_key
  FROM _t7
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
), _t2 AS (
  SELECT
    _s2.damp_modifier + 0.85 * SUM(
      CAST((
        CAST(_t8.l_source <> _t8.l_target OR _t8.l_target IS NULL AS INTEGER) * _s2.anything_page_rank
      ) AS REAL) / _s2.n_out
    ) OVER (PARTITION BY _s5.s_key) AS page_rank_0,
    NOT _t8.l_target IS NULL AND _t8.l_source = _t8.l_target AS dummy_link,
    _s5.s_key
  FROM _s2 AS _s2
  JOIN _s1 AS _t8
    ON _s2.anything_s_key = _t8.l_source
  JOIN _t7 AS _s5
    ON _s5.s_key = _t8.l_target OR _t8.l_target IS NULL
)
SELECT
  s_key AS key,
  ROUND(page_rank_0, 5) AS page_rank
FROM _t2
WHERE
  dummy_link
ORDER BY
  s_key
