WITH _t17 AS (
  SELECT
    s_key
  FROM main.sites
), _s0 AS (
  SELECT
    COUNT(*) OVER () AS n,
    s_key
  FROM _t17
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
), _t12 AS (
  SELECT
    (
      CAST(0.15000000000000002 AS REAL) / _s2.anything_n
    ) + 0.85 * SUM(
      CAST((
        CAST(_t18.l_source <> _t18.l_target OR _t18.l_target IS NULL AS INTEGER) * _s2.page_rank
      ) AS REAL) / _s2.n_out
    ) OVER (PARTITION BY _s5.s_key) AS page_rank_0,
    _s2.anything_n,
    NOT _t18.l_target IS NULL AND _t18.l_source = _t18.l_target AS dummy_link,
    _s2.n_out,
    _s5.s_key
  FROM _s2 AS _s2
  JOIN _s1 AS _t18
    ON _s2.anything_s_key = _t18.l_source
  JOIN _t17 AS _s5
    ON _s5.s_key = _t18.l_target OR _t18.l_target IS NULL
), _t10 AS (
  SELECT
    (
      CAST(0.15000000000000002 AS REAL) / _t12.anything_n
    ) + 0.85 * SUM(
      CAST((
        CAST(_t19.l_source <> _t19.l_target OR _t19.l_target IS NULL AS INTEGER) * _t12.page_rank_0
      ) AS REAL) / _t12.n_out
    ) OVER (PARTITION BY _s9.s_key) AS page_rank_0_550,
    _t12.anything_n,
    NOT _t19.l_target IS NULL AND _t19.l_source = _t19.l_target AS dummy_link_548,
    _t12.n_out,
    _s9.s_key
  FROM _t12 AS _t12
  JOIN _s1 AS _t19
    ON _t12.s_key = _t19.l_source
  JOIN _t17 AS _s9
    ON _s9.s_key = _t19.l_target OR _t19.l_target IS NULL
  WHERE
    _t12.dummy_link
), _t8 AS (
  SELECT
    (
      CAST(0.15000000000000002 AS REAL) / _t10.anything_n
    ) + 0.85 * SUM(
      CAST((
        CAST(_t20.l_source <> _t20.l_target OR _t20.l_target IS NULL AS INTEGER) * _t10.page_rank_0_550
      ) AS REAL) / _t10.n_out
    ) OVER (PARTITION BY _s13.s_key) AS page_rank_0_560,
    _t10.anything_n,
    NOT _t20.l_target IS NULL AND _t20.l_source = _t20.l_target AS dummy_link_558,
    _t10.n_out,
    _s13.s_key
  FROM _t10 AS _t10
  JOIN _s1 AS _t20
    ON _t10.s_key = _t20.l_source
  JOIN _t17 AS _s13
    ON _s13.s_key = _t20.l_target OR _t20.l_target IS NULL
  WHERE
    _t10.dummy_link_548
), _t6 AS (
  SELECT
    (
      CAST(0.15000000000000002 AS REAL) / _t8.anything_n
    ) + 0.85 * SUM(
      CAST((
        CAST(_t21.l_source <> _t21.l_target OR _t21.l_target IS NULL AS INTEGER) * _t8.page_rank_0_560
      ) AS REAL) / _t8.n_out
    ) OVER (PARTITION BY _s17.s_key) AS page_rank_0_570,
    _t8.anything_n,
    NOT _t21.l_target IS NULL AND _t21.l_source = _t21.l_target AS dummy_link_568,
    _t8.n_out,
    _s17.s_key
  FROM _t8 AS _t8
  JOIN _s1 AS _t21
    ON _t21.l_source = _t8.s_key
  JOIN _t17 AS _s17
    ON _s17.s_key = _t21.l_target OR _t21.l_target IS NULL
  WHERE
    _t8.dummy_link_558
), _t4 AS (
  SELECT
    (
      CAST(0.15000000000000002 AS REAL) / _t6.anything_n
    ) + 0.85 * SUM(
      CAST((
        CAST(_t22.l_source <> _t22.l_target OR _t22.l_target IS NULL AS INTEGER) * _t6.page_rank_0_570
      ) AS REAL) / _t6.n_out
    ) OVER (PARTITION BY _s21.s_key) AS page_rank_0_580,
    _t6.anything_n,
    NOT _t22.l_target IS NULL AND _t22.l_source = _t22.l_target AS dummy_link_578,
    _t6.n_out,
    _s21.s_key
  FROM _t6 AS _t6
  JOIN _s1 AS _t22
    ON _t22.l_source = _t6.s_key
  JOIN _t17 AS _s21
    ON _s21.s_key = _t22.l_target OR _t22.l_target IS NULL
  WHERE
    _t6.dummy_link_568
), _t2 AS (
  SELECT
    (
      CAST(0.15000000000000002 AS REAL) / _t4.anything_n
    ) + 0.85 * SUM(
      CAST((
        CAST(_t23.l_source <> _t23.l_target OR _t23.l_target IS NULL AS INTEGER) * _t4.page_rank_0_580
      ) AS REAL) / _t4.n_out
    ) OVER (PARTITION BY _s25.s_key) AS page_rank_0_590,
    NOT _t23.l_target IS NULL AND _t23.l_source = _t23.l_target AS dummy_link_588,
    _s25.s_key
  FROM _t4 AS _t4
  JOIN _s1 AS _t23
    ON _t23.l_source = _t4.s_key
  JOIN _t17 AS _s25
    ON _s25.s_key = _t23.l_target OR _t23.l_target IS NULL
  WHERE
    _t4.dummy_link_578
)
SELECT
  s_key AS key,
  ROUND(page_rank_0_590, 5) AS page_rank
FROM _t2
WHERE
  dummy_link_588
ORDER BY
  s_key
