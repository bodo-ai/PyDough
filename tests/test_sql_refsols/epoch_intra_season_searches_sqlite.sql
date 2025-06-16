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
    COUNT() AS agg_0,
    _s2.s_name AS name,
    searches.search_id
  FROM _s0 AS _s2
  JOIN searches AS searches
    ON _s2.s_month1 = CAST(STRFTIME('%m', searches.search_ts) AS INTEGER)
    OR _s2.s_month2 = CAST(STRFTIME('%m', searches.search_ts) AS INTEGER)
    OR _s2.s_month3 = CAST(STRFTIME('%m', searches.search_ts) AS INTEGER)
  JOIN _s5 AS _s5
    ON LOWER(searches.search_string) LIKE (
      '%' || LOWER(_s5.ev_name) || '%'
    )
  JOIN _s0 AS _s7
    ON _s2.s_name = _s7.s_name
    AND (
      _s7.s_month1 = CAST(STRFTIME('%m', _s5.ev_dt) AS INTEGER)
      OR _s7.s_month2 = CAST(STRFTIME('%m', _s5.ev_dt) AS INTEGER)
      OR _s7.s_month3 = CAST(STRFTIME('%m', _s5.ev_dt) AS INTEGER)
    )
  GROUP BY
    _s2.s_name,
    searches.search_id
), _s16 AS (
  SELECT
    MAX(_s0.s_name) AS agg_1,
    SUM((
      NOT _s9.agg_0 IS NULL AND _s9.agg_0 > 0
    )) AS agg_2,
    COUNT() AS agg_3
  FROM _s0 AS _s0
  JOIN searches AS searches
    ON _s0.s_month1 = CAST(STRFTIME('%m', searches.search_ts) AS INTEGER)
    OR _s0.s_month2 = CAST(STRFTIME('%m', searches.search_ts) AS INTEGER)
    OR _s0.s_month3 = CAST(STRFTIME('%m', searches.search_ts) AS INTEGER)
  LEFT JOIN _s9 AS _s9
    ON _s0.s_name = _s9.name AND _s9.search_id = searches.search_id
  GROUP BY
    _s0.s_name
), _s17 AS (
  SELECT
    SUM(_s15.s_name = _s10.s_name) AS agg_0,
    _s10.s_name AS name
  FROM _s0 AS _s10
  JOIN _s5 AS _s11
    ON _s10.s_month1 = CAST(STRFTIME('%m', _s11.ev_dt) AS INTEGER)
    OR _s10.s_month2 = CAST(STRFTIME('%m', _s11.ev_dt) AS INTEGER)
    OR _s10.s_month3 = CAST(STRFTIME('%m', _s11.ev_dt) AS INTEGER)
  JOIN searches AS searches
    ON LOWER(searches.search_string) LIKE (
      '%' || LOWER(_s11.ev_name) || '%'
    )
  JOIN _s0 AS _s15
    ON _s15.s_month1 = CAST(STRFTIME('%m', searches.search_ts) AS INTEGER)
    OR _s15.s_month2 = CAST(STRFTIME('%m', searches.search_ts) AS INTEGER)
    OR _s15.s_month3 = CAST(STRFTIME('%m', searches.search_ts) AS INTEGER)
  GROUP BY
    _s10.s_name
)
SELECT
  _s16.agg_1 AS season_name,
  ROUND(CAST((
    100.0 * COALESCE(_s16.agg_2, 0)
  ) AS REAL) / _s16.agg_3, 2) AS pct_season_searches,
  ROUND(CAST((
    100.0 * COALESCE(_s17.agg_0, 0)
  ) AS REAL) / COALESCE(_s16.agg_1, 0), 2) AS pct_event_searches
FROM _s16 AS _s16
LEFT JOIN _s17 AS _s17
  ON _s16.agg_1 = _s17.name
ORDER BY
  season_name
