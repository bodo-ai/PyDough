WITH _t0 AS (
  SELECT
    COUNT() AS agg_0,
    events.ev_typ AS event_type
  FROM seasons AS seasons
  JOIN events AS events
    ON seasons.s_month1 = CAST(STRFTIME('%m', events.ev_dt) AS INTEGER)
    OR seasons.s_month2 = CAST(STRFTIME('%m', events.ev_dt) AS INTEGER)
    OR seasons.s_month3 = CAST(STRFTIME('%m', events.ev_dt) AS INTEGER)
  WHERE
    seasons.s_name = 'Summer'
  GROUP BY
    events.ev_typ
)
SELECT
  event_type,
  COALESCE(agg_0, 0) AS n_events
FROM _t0
ORDER BY
  event_type
