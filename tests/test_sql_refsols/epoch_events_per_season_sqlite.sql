WITH _s3 AS (
  SELECT
    COUNT() AS agg_0,
    seasons.s_name AS name
  FROM seasons AS seasons
  JOIN events AS events
    ON seasons.s_month1 = CAST(STRFTIME('%m', events.ev_dt) AS INTEGER)
    OR seasons.s_month2 = CAST(STRFTIME('%m', events.ev_dt) AS INTEGER)
    OR seasons.s_month3 = CAST(STRFTIME('%m', events.ev_dt) AS INTEGER)
  GROUP BY
    seasons.s_name
)
SELECT
  seasons.s_name AS season_name,
  COALESCE(_s3.agg_0, 0) AS n_events
FROM seasons AS seasons
LEFT JOIN _s3 AS _s3
  ON _s3.name = seasons.s_name
ORDER BY
  n_events DESC,
  season_name
