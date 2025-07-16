WITH _t8 AS (
  SELECT
    s_key
  FROM main.sites
), _s0 AS (
  SELECT
    COUNT(*) OVER () AS n,
    s_key
  FROM _t8
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
), _t3 AS (
  SELECT
    (
      CAST(0.15000000000000002 AS REAL) / _s2.anything_n
    ) + 0.85 * SUM(
      CAST((
        CAST(_t9.l_source <> _t9.l_target OR _t9.l_target IS NULL AS INTEGER) * _s2.page_rank
      ) AS REAL) / _s2.n_out
    ) OVER (PARTITION BY _s5.s_key) AS page_rank_0,
    _s5.s_key
  FROM _s2 AS _s2
  JOIN _s1 AS _t9
    ON _s2.anything_s_key = _t9.l_source
  JOIN _t8 AS _s5
    ON _s5.s_key = _t9.l_target OR _t9.l_target IS NULL
), _t AS (
  SELECT
    page_rank_0,
    s_key,
    ROW_NUMBER() OVER (PARTITION BY s_key ORDER BY s_key) AS _w
  FROM _t3
)
SELECT
  s_key AS key,
  ROUND(page_rank_0, 5) AS page_rank
FROM _t
WHERE
  _w = 1
ORDER BY
  s_key
