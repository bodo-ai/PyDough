WITH _t32 AS (
  SELECT
    s_key
  FROM main.sites
), _s0 AS (
  SELECT
    COUNT(*) OVER () AS n,
    s_key
  FROM _t32
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
), _t27 AS (
  SELECT
    (
      CAST(0.15000000000000002 AS REAL) / _s2.anything_n
    ) + 0.85 * SUM(
      CAST((
        CAST(_t33.l_source <> _t33.l_target OR _t33.l_target IS NULL AS INTEGER) * _s2.page_rank
      ) AS REAL) / _s2.n_out
    ) OVER (PARTITION BY _s5.s_key) AS page_rank_0,
    _s2.anything_n,
    _s5.s_key
  FROM _s2 AS _s2
  JOIN _s1 AS _t33
    ON _s2.anything_s_key = _t33.l_source
  JOIN _t32 AS _s5
    ON _s5.s_key = _t33.l_target OR _t33.l_target IS NULL
), _t AS (
  SELECT
    page_rank_0,
    anything_n,
    s_key,
    ROW_NUMBER() OVER (PARTITION BY s_key ORDER BY s_key) AS _w
  FROM _t27
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
), _t21 AS (
  SELECT
    (
      CAST(0.15000000000000002 AS REAL) / _s8.anything_anything_n
    ) + 0.85 * SUM(
      CAST((
        CAST(_t34.l_source <> _t34.l_target OR _t34.l_target IS NULL AS INTEGER) * _s8.page_rank
      ) AS REAL) / _s8.n_out
    ) OVER (PARTITION BY _s11.s_key) AS page_rank_0,
    _s8.anything_anything_n,
    _s11.s_key
  FROM _s8 AS _s8
  JOIN _s1 AS _t34
    ON _s8.anything_s_key = _t34.l_source
  JOIN _t32 AS _s11
    ON _s11.s_key = _t34.l_target OR _t34.l_target IS NULL
), _t_2 AS (
  SELECT
    page_rank_0,
    anything_anything_n,
    s_key,
    ROW_NUMBER() OVER (PARTITION BY s_key ORDER BY s_key) AS _w
  FROM _t21
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
), _t15 AS (
  SELECT
    (
      CAST(0.15000000000000002 AS REAL) / _s14.anything_anything_anything_n
    ) + 0.85 * SUM(
      CAST((
        CAST(_t35.l_source <> _t35.l_target OR _t35.l_target IS NULL AS INTEGER) * _s14.page_rank
      ) AS REAL) / _s14.n_out
    ) OVER (PARTITION BY _s17.s_key) AS page_rank_0,
    _s14.anything_anything_anything_n,
    _s17.s_key
  FROM _s14 AS _s14
  JOIN _s1 AS _t35
    ON _s14.anything_s_key = _t35.l_source
  JOIN _t32 AS _s17
    ON _s17.s_key = _t35.l_target OR _t35.l_target IS NULL
), _t_3 AS (
  SELECT
    page_rank_0,
    anything_anything_anything_n,
    s_key,
    ROW_NUMBER() OVER (PARTITION BY s_key ORDER BY s_key) AS _w
  FROM _t15
), _s20 AS (
  SELECT
    COALESCE(
      SUM(
        IIF(
          _s19.l_target IS NULL,
          _t.anything_anything_anything_n,
          CAST(_s19.l_source <> _s19.l_target AS INTEGER)
        )
      ),
      0
    ) AS n_out,
    MAX(_t.page_rank_0) AS page_rank,
    MAX(_t.anything_anything_anything_n) AS anything_anything_anything_anything_n,
    MAX(_t.s_key) AS anything_s_key
  FROM _t_3 AS _t
  JOIN _s1 AS _s19
    ON _s19.l_source = _t.s_key
  WHERE
    _t._w = 1
  GROUP BY
    _t.s_key
), _t9 AS (
  SELECT
    (
      CAST(0.15000000000000002 AS REAL) / _s20.anything_anything_anything_anything_n
    ) + 0.85 * SUM(
      CAST((
        CAST(_t36.l_source <> _t36.l_target OR _t36.l_target IS NULL AS INTEGER) * _s20.page_rank
      ) AS REAL) / _s20.n_out
    ) OVER (PARTITION BY _s23.s_key) AS page_rank_0,
    _s20.anything_anything_anything_anything_n,
    _s23.s_key
  FROM _s20 AS _s20
  JOIN _s1 AS _t36
    ON _s20.anything_s_key = _t36.l_source
  JOIN _t32 AS _s23
    ON _s23.s_key = _t36.l_target OR _t36.l_target IS NULL
), _t_4 AS (
  SELECT
    page_rank_0,
    anything_anything_anything_anything_n,
    s_key,
    ROW_NUMBER() OVER (PARTITION BY s_key ORDER BY s_key) AS _w
  FROM _t9
), _s26 AS (
  SELECT
    COALESCE(
      SUM(
        IIF(
          _s25.l_target IS NULL,
          _t.anything_anything_anything_anything_n,
          CAST(_s25.l_source <> _s25.l_target AS INTEGER)
        )
      ),
      0
    ) AS n_out,
    MAX(_t.page_rank_0) AS page_rank,
    MAX(_t.anything_anything_anything_anything_n) AS anything_anything_anything_anything_anything_n,
    MAX(_t.s_key) AS anything_s_key
  FROM _t_4 AS _t
  JOIN _s1 AS _s25
    ON _s25.l_source = _t.s_key
  WHERE
    _t._w = 1
  GROUP BY
    _t.s_key
), _t3 AS (
  SELECT
    (
      CAST(0.15000000000000002 AS REAL) / _s26.anything_anything_anything_anything_anything_n
    ) + 0.85 * SUM(
      CAST((
        CAST(_t37.l_source <> _t37.l_target OR _t37.l_target IS NULL AS INTEGER) * _s26.page_rank
      ) AS REAL) / _s26.n_out
    ) OVER (PARTITION BY _s29.s_key) AS page_rank_0,
    _s29.s_key
  FROM _s26 AS _s26
  JOIN _s1 AS _t37
    ON _s26.anything_s_key = _t37.l_source
  JOIN _t32 AS _s29
    ON _s29.s_key = _t37.l_target OR _t37.l_target IS NULL
), _t_5 AS (
  SELECT
    page_rank_0,
    s_key,
    ROW_NUMBER() OVER (PARTITION BY s_key ORDER BY s_key) AS _w
  FROM _t3
)
SELECT
  s_key AS key,
  ROUND(page_rank_0, 5) AS page_rank
FROM _t_5
WHERE
  _w = 1
ORDER BY
  s_key
