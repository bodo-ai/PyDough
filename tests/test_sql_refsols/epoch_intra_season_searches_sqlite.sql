WITH _s0 AS (
  SELECT
    s_month1 AS first_month,
    s_name AS name,
    s_name AS season_name,
    s_month2 AS second_month,
    s_month3 AS third_month
  FROM seasons
), _s5 AS (
  SELECT
    ev_dt AS date_time,
    ev_name AS name
  FROM events
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
    ON _s2.first_month = CAST(STRFTIME('%m', searches.search_ts) AS INTEGER)
    OR _s2.second_month = CAST(STRFTIME('%m', searches.search_ts) AS INTEGER)
    OR _s2.third_month = CAST(STRFTIME('%m', searches.search_ts) AS INTEGER)
  JOIN _s5 AS _s5
    ON LOWER(searches.search_string) LIKE (
      '%' || LOWER(_s5.name) || '%'
    )
  JOIN _s7 AS _s7
    ON _s2.season_name = _s7.name
    AND (
      _s7.first_month = CAST(STRFTIME('%m', _s5.date_time) AS INTEGER)
      OR _s7.second_month = CAST(STRFTIME('%m', _s5.date_time) AS INTEGER)
      OR _s7.third_month = CAST(STRFTIME('%m', _s5.date_time) AS INTEGER)
    )
  GROUP BY
    _s2.name,
    searches.search_id
), _s18 AS (
  SELECT
    MAX(_s0.name) AS agg_1,
    SUM((
      NOT _s9.agg_0 IS NULL AND _s9.agg_0 > 0
    )) AS agg_2,
    COUNT() AS agg_3,
    MAX(_s0.season_name) AS agg_4
  FROM _s0 AS _s0
  JOIN searches AS searches
    ON _s0.first_month = CAST(STRFTIME('%m', searches.search_ts) AS INTEGER)
    OR _s0.second_month = CAST(STRFTIME('%m', searches.search_ts) AS INTEGER)
    OR _s0.third_month = CAST(STRFTIME('%m', searches.search_ts) AS INTEGER)
  LEFT JOIN _s9 AS _s9
    ON _s0.name = _s9.name AND _s9.search_id = searches.search_id
  GROUP BY
    _s0.name
), _s12 AS (
  SELECT
    MAX(_s10.first_month) AS agg_0,
    MAX(_s10.name) AS agg_1,
    MAX(_s10.season_name) AS agg_4,
    MAX(_s10.second_month) AS agg_5,
    MAX(_s10.third_month) AS agg_6
  FROM _s0 AS _s10
  JOIN searches AS searches
    ON _s10.first_month = CAST(STRFTIME('%m', searches.search_ts) AS INTEGER)
    OR _s10.second_month = CAST(STRFTIME('%m', searches.search_ts) AS INTEGER)
    OR _s10.third_month = CAST(STRFTIME('%m', searches.search_ts) AS INTEGER)
  GROUP BY
    _s10.name
), _s19 AS (
  SELECT
    SUM(_s12.agg_4 = _s17.name) AS agg_0,
    COUNT() AS agg_1_15,
    _s12.agg_1
  FROM _s12 AS _s12
  JOIN _s5 AS _s13
    ON _s12.agg_0 = CAST(STRFTIME('%m', _s13.date_time) AS INTEGER)
    OR _s12.agg_5 = CAST(STRFTIME('%m', _s13.date_time) AS INTEGER)
    OR _s12.agg_6 = CAST(STRFTIME('%m', _s13.date_time) AS INTEGER)
  JOIN searches AS searches
    ON LOWER(searches.search_string) LIKE (
      '%' || LOWER(_s13.name) || '%'
    )
  JOIN _s7 AS _s17
    ON _s17.first_month = CAST(STRFTIME('%m', searches.search_ts) AS INTEGER)
    OR _s17.second_month = CAST(STRFTIME('%m', searches.search_ts) AS INTEGER)
    OR _s17.third_month = CAST(STRFTIME('%m', searches.search_ts) AS INTEGER)
  GROUP BY
    _s12.agg_1
)
SELECT
  _s18.agg_4 AS season_name,
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
