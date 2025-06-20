WITH _s15 AS (
  SELECT
    COUNT(*) AS agg_0,
    _s4.s_name AS name,
    _s5.search_id
  FROM seasons AS _s4
  JOIN searches AS _s5
    ON _s4.s_month1 = EXTRACT(MONTH FROM _s5.search_ts)
    OR _s4.s_month2 = EXTRACT(MONTH FROM _s5.search_ts)
    OR _s4.s_month3 = EXTRACT(MONTH FROM _s5.search_ts)
  JOIN events AS _s8
    ON LOWER(_s5.search_string) LIKE CONCAT('%', LOWER(_s8.ev_name), '%')
  JOIN seasons AS _s11
    ON (
      _s11.s_month1 = EXTRACT(MONTH FROM _s8.ev_dt)
      OR _s11.s_month2 = EXTRACT(MONTH FROM _s8.ev_dt)
      OR _s11.s_month3 = EXTRACT(MONTH FROM _s8.ev_dt)
    )
    AND _s11.s_name = _s4.s_name
  GROUP BY
    _s4.s_name,
    _s5.search_id
), _s26 AS (
  SELECT
    ANY_VALUE(_s0.s_name) AS agg_1,
    SUM((
      NOT _s15.agg_0 IS NULL AND _s15.agg_0 > 0
    )) AS agg_2,
    COUNT(*) AS agg_3,
    ANY_VALUE(_s0.s_name) AS agg_4
  FROM seasons AS _s0
  JOIN searches AS _s1
    ON _s0.s_month1 = EXTRACT(MONTH FROM _s1.search_ts)
    OR _s0.s_month2 = EXTRACT(MONTH FROM _s1.search_ts)
    OR _s0.s_month3 = EXTRACT(MONTH FROM _s1.search_ts)
  LEFT JOIN _s15 AS _s15
    ON _s0.s_name = _s15.name AND _s1.search_id = _s15.search_id
  GROUP BY
    _s0.s_name
), _s27 AS (
  SELECT
    SUM(_s23.s_name = _s16.s_name) AS agg_0,
    COUNT(*) AS agg_1,
    _s16.s_name AS name
  FROM seasons AS _s16
  JOIN events AS _s17
    ON _s16.s_month1 = EXTRACT(MONTH FROM _s17.ev_dt)
    OR _s16.s_month2 = EXTRACT(MONTH FROM _s17.ev_dt)
    OR _s16.s_month3 = EXTRACT(MONTH FROM _s17.ev_dt)
  JOIN searches AS _s20
    ON LOWER(_s20.search_string) LIKE CONCAT('%', LOWER(_s17.ev_name), '%')
  JOIN seasons AS _s23
    ON _s23.s_month1 = EXTRACT(MONTH FROM _s20.search_ts)
    OR _s23.s_month2 = EXTRACT(MONTH FROM _s20.search_ts)
    OR _s23.s_month3 = EXTRACT(MONTH FROM _s20.search_ts)
  GROUP BY
    _s16.s_name
)
SELECT
  _s26.agg_4 AS season_name,
  ROUND((
    100.0 * COALESCE(_s26.agg_2, 0)
  ) / _s26.agg_3, 2) AS pct_season_searches,
  ROUND((
    100.0 * COALESCE(_s27.agg_0, 0)
  ) / COALESCE(_s27.agg_1, 0), 2) AS pct_event_searches
FROM _s26 AS _s26
LEFT JOIN _s27 AS _s27
  ON _s26.agg_1 = _s27.name
ORDER BY
  season_name
