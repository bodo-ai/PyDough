WITH _s0 AS (
  SELECT
    s_month1 AS first_month,
    s_name AS name,
    s_month2 AS second_month,
    s_month3 AS third_month
  FROM seasons
), _s2 AS (
  SELECT
    s_month1 AS first_month,
    s_name AS name,
    s_name AS season_name,
    s_month2 AS second_month,
    s_month3 AS third_month
  FROM seasons
), _s9 AS (
  SELECT
    COUNT() AS agg_0,
    _s2.name,
    searches.search_id
  FROM _s2 AS _s2
  JOIN searches AS searches
    ON _s2.first_month = CAST(STRFTIME('%m', searches.search_ts) AS INTEGER)
    OR _s2.second_month = CAST(STRFTIME('%m', searches.search_ts) AS INTEGER)
    OR _s2.third_month = CAST(STRFTIME('%m', searches.search_ts) AS INTEGER)
  JOIN events AS events
    ON LOWER(searches.search_string) LIKE (
      '%' || LOWER(events.ev_name) || '%'
    )
  LEFT JOIN _s0 AS _s7
    ON _s7.first_month = CAST(STRFTIME('%m', events.ev_dt) AS INTEGER)
    OR _s7.second_month = CAST(STRFTIME('%m', events.ev_dt) AS INTEGER)
    OR _s7.third_month = CAST(STRFTIME('%m', events.ev_dt) AS INTEGER)
  WHERE
    _s2.season_name = _s7.name
  GROUP BY
    _s2.name,
    searches.search_id
), _s11 AS (
  SELECT
    COUNT() AS agg_3,
    SUM((
      NOT _s9.agg_0 IS NULL AND _s9.agg_0 > 0
    )) AS agg_2,
    _s0.name
  FROM _s0 AS _s0
  JOIN searches AS searches
    ON _s0.first_month = CAST(STRFTIME('%m', searches.search_ts) AS INTEGER)
    OR _s0.second_month = CAST(STRFTIME('%m', searches.search_ts) AS INTEGER)
    OR _s0.third_month = CAST(STRFTIME('%m', searches.search_ts) AS INTEGER)
  LEFT JOIN _s9 AS _s9
    ON _s0.name = _s9.name AND _s9.search_id = searches.search_id
  GROUP BY
    _s0.name
), _s19 AS (
  SELECT
    COUNT() AS agg_1,
    SUM(_s12.season_name = _s17.name) AS agg_0,
    _s12.name
  FROM _s2 AS _s12
  JOIN events AS events
    ON _s12.first_month = CAST(STRFTIME('%m', events.ev_dt) AS INTEGER)
    OR _s12.second_month = CAST(STRFTIME('%m', events.ev_dt) AS INTEGER)
    OR _s12.third_month = CAST(STRFTIME('%m', events.ev_dt) AS INTEGER)
  JOIN searches AS searches
    ON LOWER(searches.search_string) LIKE (
      '%' || LOWER(events.ev_name) || '%'
    )
  LEFT JOIN _s0 AS _s17
    ON _s17.first_month = CAST(STRFTIME('%m', searches.search_ts) AS INTEGER)
    OR _s17.second_month = CAST(STRFTIME('%m', searches.search_ts) AS INTEGER)
    OR _s17.third_month = CAST(STRFTIME('%m', searches.search_ts) AS INTEGER)
  GROUP BY
    _s12.name
)
SELECT
  seasons.s_name AS season_name,
  ROUND(CAST((
    100.0 * COALESCE(_s11.agg_2, 0)
  ) AS REAL) / COALESCE(_s11.agg_3, 0), 2) AS pct_season_searches,
  ROUND(CAST((
    100.0 * COALESCE(_s19.agg_0, 0)
  ) AS REAL) / COALESCE(_s19.agg_1, 0), 2) AS pct_event_searches
FROM seasons AS seasons
LEFT JOIN _s11 AS _s11
  ON _s11.name = seasons.s_name
LEFT JOIN _s19 AS _s19
  ON _s19.name = seasons.s_name
ORDER BY
  seasons.s_name
