WITH _s0 AS (
  SELECT
    s_month1,
    s_month2,
    s_month3,
    s_name
  FROM seasons
), _s8 AS (
  SELECT
    _s0.s_name,
    COUNT(*) AS n_rows,
    SUM(1) AS sum_is_intra_season
  FROM _s0 AS _s0
  JOIN searches AS searches
    ON _s0.s_month1 = EXTRACT(MONTH FROM CAST(searches.search_ts AS TIMESTAMP))
    OR _s0.s_month2 = EXTRACT(MONTH FROM CAST(searches.search_ts AS TIMESTAMP))
    OR _s0.s_month3 = EXTRACT(MONTH FROM CAST(searches.search_ts AS TIMESTAMP))
  GROUP BY
    1
), _s9 AS (
  SELECT
    _s2.s_name,
    COUNT(*) AS n_rows,
    SUM(CASE WHEN _s7.s_name = _s2.s_name THEN 1 ELSE 0 END) AS sum_is_intra_season
  FROM _s0 AS _s2
  JOIN events AS events
    ON _s2.s_month1 = EXTRACT(MONTH FROM CAST(events.ev_dt AS TIMESTAMP))
    OR _s2.s_month2 = EXTRACT(MONTH FROM CAST(events.ev_dt AS TIMESTAMP))
    OR _s2.s_month3 = EXTRACT(MONTH FROM CAST(events.ev_dt AS TIMESTAMP))
  JOIN searches AS searches
    ON LOWER(searches.search_string) LIKE CONCAT('%', LOWER(events.ev_name), '%')
  JOIN _s0 AS _s7
    ON _s7.s_month1 = EXTRACT(MONTH FROM CAST(searches.search_ts AS TIMESTAMP))
    OR _s7.s_month2 = EXTRACT(MONTH FROM CAST(searches.search_ts AS TIMESTAMP))
    OR _s7.s_month3 = EXTRACT(MONTH FROM CAST(searches.search_ts AS TIMESTAMP))
  GROUP BY
    1
)
SELECT
  _s8.s_name AS season_name,
  ROUND(CAST((
    100.0 * _s8.sum_is_intra_season
  ) / _s8.n_rows AS DECIMAL), 2) AS pct_season_searches,
  ROUND(
    CAST((
      100.0 * COALESCE(_s9.sum_is_intra_season, 0)
    ) / _s9.n_rows AS DECIMAL),
    2
  ) AS pct_event_searches
FROM _s8 AS _s8
JOIN _s9 AS _s9
  ON _s8.s_name = _s9.s_name
ORDER BY
  1 NULLS FIRST
