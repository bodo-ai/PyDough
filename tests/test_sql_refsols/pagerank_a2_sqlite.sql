WITH _t14 AS (
  SELECT
    s_key
  FROM main.sites
), _s0 AS (
  SELECT
    COUNT(*) OVER () AS n,
    s_key
  FROM _t14
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
), _t9 AS (
  SELECT
    (
      CAST(0.15000000000000002 AS REAL) / _s2.anything_n
    ) + 0.85 * SUM(
      CAST((
        CAST(_t15.l_source <> _t15.l_target OR _t15.l_target IS NULL AS INTEGER) * _s2.page_rank
      ) AS REAL) / _s2.n_out
    ) OVER (PARTITION BY _s5.s_key) AS page_rank_0,
    _s2.anything_n,
    _s5.s_key
  FROM _s2 AS _s2
  JOIN _s1 AS _t15
    ON _s2.anything_s_key = _t15.l_source
  JOIN _t14 AS _s5
    ON _s5.s_key = _t15.l_target OR _t15.l_target IS NULL
), _t AS (
  SELECT
    page_rank_0,
    anything_n,
    s_key,
    ROW_NUMBER() OVER (PARTITION BY s_key ORDER BY s_key) AS _w
  FROM _t9
), _s8 AS (
  SELECT
    COALESCE(
      SUM(
        IIF(_s7.l_target IS NULL, _t.anything_n, CAST(_s7.l_source <> _s7.l_target AS INTEGER))
      ),
      0
    ) AS n_out,
    MAX(_t.page_rank_0) AS page_rank,
    MAX(_t.anything_n) AS anything_anything_n,
    MAX(_t.s_key) AS anything_s_key
  FROM _t AS _t
  JOIN _s1 AS _s7
    ON _s7.l_source = _t.s_key
  WHERE
    _t._w = 1
  GROUP BY
    _t.s_key
), _t3 AS (
  SELECT
    (
      CAST(0.15000000000000002 AS REAL) / _s8.anything_anything_n
    ) + 0.85 * SUM(
      CAST((
        CAST(_t16.l_source <> _t16.l_target OR _t16.l_target IS NULL AS INTEGER) * _s8.page_rank
      ) AS REAL) / _s8.n_out
    ) OVER (PARTITION BY _s11.s_key) AS page_rank_0,
    _s11.s_key
  FROM _s8 AS _s8
  JOIN _s1 AS _t16
    ON _s8.anything_s_key = _t16.l_source
  JOIN _t14 AS _s11
    ON _s11.s_key = _t16.l_target OR _t16.l_target IS NULL
), _t_2 AS (
  SELECT
    page_rank_0,
    s_key,
    ROW_NUMBER() OVER (PARTITION BY s_key ORDER BY s_key) AS _w
  FROM _t3
)
SELECT
  s_key AS key,
  ROUND(page_rank_0, 5) AS page_rank
FROM _t_2
WHERE
  _w = 1
ORDER BY
  s_key
