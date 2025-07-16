WITH _t20 AS (
  SELECT
    s_key
  FROM main.sites
), _s0 AS (
  SELECT
    COUNT(*) OVER () AS n,
    s_key
  FROM _t20
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
), _t15 AS (
  SELECT
    (
      CAST(0.15000000000000002 AS REAL) / _s2.anything_n
    ) + 0.85 * SUM(
      CAST((
        CAST(_t21.l_source <> _t21.l_target OR _t21.l_target IS NULL AS INTEGER) * _s2.page_rank
      ) AS REAL) / _s2.n_out
    ) OVER (PARTITION BY _s5.s_key) AS page_rank_0,
    _s2.anything_n,
    _s5.s_key
  FROM _s2 AS _s2
  JOIN _s1 AS _t21
    ON _s2.anything_s_key = _t21.l_source
  JOIN _t20 AS _s5
    ON _s5.s_key = _t21.l_target OR _t21.l_target IS NULL
), _t AS (
  SELECT
    page_rank_0,
    anything_n,
    s_key,
    ROW_NUMBER() OVER (PARTITION BY s_key ORDER BY s_key) AS _w
  FROM _t15
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
), _t9 AS (
  SELECT
    (
      CAST(0.15000000000000002 AS REAL) / _s8.anything_anything_n
    ) + 0.85 * SUM(
      CAST((
        CAST(_t22.l_source <> _t22.l_target OR _t22.l_target IS NULL AS INTEGER) * _s8.page_rank
      ) AS REAL) / _s8.n_out
    ) OVER (PARTITION BY _s11.s_key) AS page_rank_0,
    _s8.anything_anything_n,
    _s11.s_key
  FROM _s8 AS _s8
  JOIN _s1 AS _t22
    ON _s8.anything_s_key = _t22.l_source
  JOIN _t20 AS _s11
    ON _s11.s_key = _t22.l_target OR _t22.l_target IS NULL
), _t_2 AS (
  SELECT
    page_rank_0,
    anything_anything_n,
    s_key,
    ROW_NUMBER() OVER (PARTITION BY s_key ORDER BY s_key) AS _w
  FROM _t9
), _s14 AS (
  SELECT
    COALESCE(
      SUM(
        IIF(
          _s13.l_target IS NULL,
          _t.anything_anything_n,
          CAST(_s13.l_source <> _s13.l_target AS INTEGER)
        )
      ),
      0
    ) AS n_out,
    MAX(_t.page_rank_0) AS page_rank,
    MAX(_t.anything_anything_n) AS anything_anything_anything_n,
    MAX(_t.s_key) AS anything_s_key
  FROM _t_2 AS _t
  JOIN _s1 AS _s13
    ON _s13.l_source = _t.s_key
  WHERE
    _t._w = 1
  GROUP BY
    _t.s_key
), _t3 AS (
  SELECT
    (
      CAST(0.15000000000000002 AS REAL) / _s14.anything_anything_anything_n
    ) + 0.85 * SUM(
      CAST((
        CAST(_t23.l_source <> _t23.l_target OR _t23.l_target IS NULL AS INTEGER) * _s14.page_rank
      ) AS REAL) / _s14.n_out
    ) OVER (PARTITION BY _s17.s_key) AS page_rank_0,
    _s17.s_key
  FROM _s14 AS _s14
  JOIN _s1 AS _t23
    ON _s14.anything_s_key = _t23.l_source
  JOIN _t20 AS _s17
    ON _s17.s_key = _t23.l_target OR _t23.l_target IS NULL
), _t_3 AS (
  SELECT
    page_rank_0,
    s_key,
    ROW_NUMBER() OVER (PARTITION BY s_key ORDER BY s_key) AS _w
  FROM _t3
)
SELECT
  s_key AS key,
  ROUND(page_rank_0, 5) AS page_rank
FROM _t_3
WHERE
  _w = 1
ORDER BY
  s_key
