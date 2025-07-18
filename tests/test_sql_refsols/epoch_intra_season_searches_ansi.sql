WITH _s0 AS (
  SELECT
    s_month1,
    s_month2,
    s_month3,
    s_name
  FROM seasons
), _s5 AS (
  SELECT
    ev_dt,
    ev_name
  FROM events
), _s9 AS (
  SELECT
    COUNT(*) AS n_rows,
    _s2.s_name,
    searches.search_id
  FROM _s0 AS _s2
  JOIN searches AS searches
    ON _s2.s_month1 = EXTRACT(MONTH FROM CAST(searches.search_ts AS DATETIME))
    OR _s2.s_month2 = EXTRACT(MONTH FROM CAST(searches.search_ts AS DATETIME))
    OR _s2.s_month3 = EXTRACT(MONTH FROM CAST(searches.search_ts AS DATETIME))
  JOIN _s5 AS _s5
    ON LOWER(searches.search_string) LIKE CONCAT('%', LOWER(_s5.ev_name), '%')
  JOIN _s0 AS _s7
    ON _s2.s_name = _s7.s_name
    AND (
      _s7.s_month1 = EXTRACT(MONTH FROM CAST(_s5.ev_dt AS DATETIME))
      OR _s7.s_month2 = EXTRACT(MONTH FROM CAST(_s5.ev_dt AS DATETIME))
      OR _s7.s_month3 = EXTRACT(MONTH FROM CAST(_s5.ev_dt AS DATETIME))
    )
  GROUP BY
    _s2.s_name,
    searches.search_id
), _s16 AS (
  SELECT
    ANY_VALUE(_s0.s_name) AS anything_s_name,
    COUNT(*) AS n_rows,
    SUM((
      NOT _s9.n_rows IS NULL AND _s9.n_rows > 0
    )) AS sum_is_intra_season
  FROM _s0 AS _s0
  JOIN searches AS searches
    ON _s0.s_month1 = EXTRACT(MONTH FROM CAST(searches.search_ts AS DATETIME))
    OR _s0.s_month2 = EXTRACT(MONTH FROM CAST(searches.search_ts AS DATETIME))
    OR _s0.s_month3 = EXTRACT(MONTH FROM CAST(searches.search_ts AS DATETIME))
  LEFT JOIN _s9 AS _s9
    ON _s0.s_name = _s9.s_name AND _s9.search_id = searches.search_id
  GROUP BY
    _s0.s_name
), _s17 AS (
  SELECT
    COUNT(*) AS n_rows,
    SUM(_s15.s_name = _s10.s_name) AS sum_is_intra_season,
    _s10.s_name
  FROM _s0 AS _s10
  JOIN _s5 AS _s11
    ON _s10.s_month1 = EXTRACT(MONTH FROM CAST(_s11.ev_dt AS DATETIME))
    OR _s10.s_month2 = EXTRACT(MONTH FROM CAST(_s11.ev_dt AS DATETIME))
    OR _s10.s_month3 = EXTRACT(MONTH FROM CAST(_s11.ev_dt AS DATETIME))
  JOIN searches AS searches
    ON LOWER(searches.search_string) LIKE CONCAT('%', LOWER(_s11.ev_name), '%')
  JOIN _s0 AS _s15
    ON _s15.s_month1 = EXTRACT(MONTH FROM CAST(searches.search_ts AS DATETIME))
    OR _s15.s_month2 = EXTRACT(MONTH FROM CAST(searches.search_ts AS DATETIME))
    OR _s15.s_month3 = EXTRACT(MONTH FROM CAST(searches.search_ts AS DATETIME))
  GROUP BY
    _s10.s_name
)
SELECT
  _s16.anything_s_name AS season_name,
  ROUND((
    100.0 * COALESCE(_s16.sum_is_intra_season, 0)
  ) / _s16.n_rows, 2) AS pct_season_searches,
  ROUND((
    100.0 * COALESCE(_s17.sum_is_intra_season, 0)
  ) / COALESCE(_s17.n_rows, 0), 2) AS pct_event_searches
FROM _s16 AS _s16
LEFT JOIN _s17 AS _s17
  ON _s16.anything_s_name = _s17.s_name
ORDER BY
  _s16.anything_s_name
