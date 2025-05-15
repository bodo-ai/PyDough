WITH _s2 AS (
  SELECT
    s_month1 AS first_month,
    s_name AS name,
    s_name AS season_name,
    s_month2 AS second_month,
    s_month3 AS third_month
  FROM seasons
), _s7 AS (
  SELECT
    s_name AS name
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
  JOIN _s7 AS _s7
    ON _s2.season_name = _s7.name
  GROUP BY
    _s2.name,
    searches.search_id
), _s18 AS (
  SELECT
    MAX(seasons.s_name) AS agg_1,
    COUNT() AS agg_3,
    SUM((
      NOT _s9.agg_0 IS NULL AND _s9.agg_0 > 0
    )) AS agg_2
  FROM seasons AS seasons
  JOIN searches AS searches
    ON seasons.s_month1 = CAST(STRFTIME('%m', searches.search_ts) AS INTEGER)
    OR seasons.s_month2 = CAST(STRFTIME('%m', searches.search_ts) AS INTEGER)
    OR seasons.s_month3 = CAST(STRFTIME('%m', searches.search_ts) AS INTEGER)
  LEFT JOIN _s9 AS _s9
    ON _s9.name = seasons.s_name AND _s9.search_id = searches.search_id
  GROUP BY
    seasons.s_name
), _s12 AS (
  SELECT
    MAX(_s10.first_month) AS agg_0,
    MAX(_s10.name) AS agg_1,
    MAX(_s10.season_name) AS agg_4,
    MAX(_s10.second_month) AS agg_5,
    MAX(_s10.third_month) AS agg_6
  FROM _s2 AS _s10
  JOIN searches AS searches
    ON _s10.first_month = CAST(STRFTIME('%m', searches.search_ts) AS INTEGER)
    OR _s10.second_month = CAST(STRFTIME('%m', searches.search_ts) AS INTEGER)
    OR _s10.third_month = CAST(STRFTIME('%m', searches.search_ts) AS INTEGER)
  GROUP BY
    _s10.name
), _s19 AS (
  SELECT
    COUNT() AS agg_1_15,
    SUM(_s12.agg_4 = _s17.name) AS agg_0,
    _s12.agg_1
  FROM _s12 AS _s12
  JOIN events AS events
    ON _s12.agg_0 = CAST(STRFTIME('%m', events.ev_dt) AS INTEGER)
    OR _s12.agg_5 = CAST(STRFTIME('%m', events.ev_dt) AS INTEGER)
    OR _s12.agg_6 = CAST(STRFTIME('%m', events.ev_dt) AS INTEGER)
  JOIN searches AS searches
    ON LOWER(searches.search_string) LIKE (
      '%' || LOWER(events.ev_name) || '%'
    )
  CROSS JOIN _s7 AS _s17
  GROUP BY
    _s12.agg_1
)
SELECT
  _s18.agg_1 AS season_name,
  ROUND(CAST((
    100.0 * COALESCE(_s18.agg_2, 0)
  ) AS REAL) / _s18.agg_3, 2) AS pct_season_searches,
  ROUND(
    CAST((
      100.0 * COALESCE(_s19.agg_0, 0)
    ) AS REAL) / COALESCE(_s19.agg_1_15, 0),
    2
  ) AS pct_event_searches
FROM _s18 AS _s18
LEFT JOIN _s19 AS _s19
  ON _s18.agg_1 = _s19.agg_1
ORDER BY
  season_name
