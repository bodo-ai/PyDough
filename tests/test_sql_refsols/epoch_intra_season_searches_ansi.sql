WITH _s0 AS (
  SELECT
    s_month1 AS first_month,
    s_name AS name,
    s_name AS season_name,
    s_month2 AS second_month,
    s_month3 AS third_month
  FROM seasons
), _s7 AS (
  SELECT
    s_month1 AS first_month,
    s_name AS name,
    s_month2 AS second_month,
    s_month3 AS third_month
  FROM seasons
), _s9 AS (
  SELECT
    COUNT() AS agg_0,
    _s2.name,
    searches.search_id
  FROM _s0 AS _s2
  JOIN searches AS searches
    ON _s2.first_month = EXTRACT(MONTH FROM searches.search_ts)
    OR _s2.second_month = EXTRACT(MONTH FROM searches.search_ts)
    OR _s2.third_month = EXTRACT(MONTH FROM searches.search_ts)
  JOIN events AS events
    ON LOWER(searches.search_string) LIKE CONCAT('%', LOWER(events.ev_name), '%')
  JOIN _s7 AS _s7
    ON _s2.season_name = _s7.name
    AND (
      _s7.first_month = EXTRACT(MONTH FROM events.ev_dt)
      OR _s7.second_month = EXTRACT(MONTH FROM events.ev_dt)
      OR _s7.third_month = EXTRACT(MONTH FROM events.ev_dt)
    )
  GROUP BY
    _s2.name,
    searches.search_id
), _s16 AS (
  SELECT
    ANY_VALUE(_s0.name) AS agg_1,
    SUM((
      NOT _s9.agg_0 IS NULL AND _s9.agg_0 > 0
    )) AS agg_2,
    COUNT() AS agg_3,
    ANY_VALUE(_s0.season_name) AS agg_4
  FROM _s0 AS _s0
  JOIN searches AS searches
    ON _s0.first_month = EXTRACT(MONTH FROM searches.search_ts)
    OR _s0.second_month = EXTRACT(MONTH FROM searches.search_ts)
    OR _s0.third_month = EXTRACT(MONTH FROM searches.search_ts)
  LEFT JOIN _s9 AS _s9
    ON _s0.name = _s9.name AND _s9.search_id = searches.search_id
  GROUP BY
    _s0.name
), _s17 AS (
  SELECT
    SUM(_s10.season_name = _s15.name) AS agg_0,
    COUNT() AS agg_1,
    _s10.name
  FROM _s0 AS _s10
  JOIN events AS events
    ON _s10.first_month = EXTRACT(MONTH FROM events.ev_dt)
    OR _s10.second_month = EXTRACT(MONTH FROM events.ev_dt)
    OR _s10.third_month = EXTRACT(MONTH FROM events.ev_dt)
  JOIN searches AS searches
    ON LOWER(searches.search_string) LIKE CONCAT('%', LOWER(events.ev_name), '%')
  JOIN _s7 AS _s15
    ON _s15.first_month = EXTRACT(MONTH FROM searches.search_ts)
    OR _s15.second_month = EXTRACT(MONTH FROM searches.search_ts)
    OR _s15.third_month = EXTRACT(MONTH FROM searches.search_ts)
  GROUP BY
    _s10.name
)
SELECT
  _s16.agg_4 AS season_name,
  ROUND((
    100.0 * COALESCE(_s16.agg_2, 0)
  ) / _s16.agg_3, 2) AS pct_season_searches,
  ROUND((
    100.0 * COALESCE(_s17.agg_0, 0)
  ) / COALESCE(_s17.agg_1, 0), 2) AS pct_event_searches
FROM _s16 AS _s16
LEFT JOIN _s17 AS _s17
  ON _s16.agg_1 = _s17.name
ORDER BY
  season_name
