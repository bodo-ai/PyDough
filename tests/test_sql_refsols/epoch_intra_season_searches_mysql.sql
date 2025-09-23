WITH _s0 AS (
  SELECT
    s_month1,
    s_month2,
    s_month3,
    s_name
  FROM SEASONS
), _s8 AS (
  SELECT
    _s0.s_name,
    COUNT(*) AS n_rows,
    SUM(TRUE) AS sum_is_intra_season
  FROM _s0 AS _s0
  JOIN SEARCHES AS SEARCHES
    ON _s0.s_month1 = EXTRACT(MONTH FROM CAST(SEARCHES.search_ts AS DATETIME))
    OR _s0.s_month2 = EXTRACT(MONTH FROM CAST(SEARCHES.search_ts AS DATETIME))
    OR _s0.s_month3 = EXTRACT(MONTH FROM CAST(SEARCHES.search_ts AS DATETIME))
  GROUP BY
    1
), _s9 AS (
  SELECT
    _s2.s_name,
    COUNT(*) AS n_rows,
    SUM(_s7.s_name = _s2.s_name) AS sum_is_intra_season
  FROM _s0 AS _s2
  JOIN EVENTS AS EVENTS
    ON _s2.s_month1 = EXTRACT(MONTH FROM CAST(EVENTS.ev_dt AS DATETIME))
    OR _s2.s_month2 = EXTRACT(MONTH FROM CAST(EVENTS.ev_dt AS DATETIME))
    OR _s2.s_month3 = EXTRACT(MONTH FROM CAST(EVENTS.ev_dt AS DATETIME))
  JOIN SEARCHES AS SEARCHES
    ON LOWER(SEARCHES.search_string) LIKE CONCAT('%', LOWER(EVENTS.ev_name), '%')
  JOIN _s0 AS _s7
    ON _s7.s_month1 = EXTRACT(MONTH FROM CAST(SEARCHES.search_ts AS DATETIME))
    OR _s7.s_month2 = EXTRACT(MONTH FROM CAST(SEARCHES.search_ts AS DATETIME))
    OR _s7.s_month3 = EXTRACT(MONTH FROM CAST(SEARCHES.search_ts AS DATETIME))
  GROUP BY
    1
)
SELECT
  _s8.s_name COLLATE utf8mb4_bin AS season_name,
  ROUND((
    100.0 * _s8.sum_is_intra_season
  ) / _s8.n_rows, 2) AS pct_season_searches,
  ROUND((
    100.0 * COALESCE(_s9.sum_is_intra_season, 0)
  ) / _s9.n_rows, 2) AS pct_event_searches
FROM _s8 AS _s8
JOIN _s9 AS _s9
  ON _s8.s_name = _s9.s_name
ORDER BY
  1
