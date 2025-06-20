WITH _t0 AS (
  SELECT
    SUM(DATEDIFF(session_end_ts, session_start_ts, SECOND)) AS agg_0,
    user_id
  FROM main.user_sessions
  WHERE
    session_end_ts < '2023-06-08' AND session_start_ts >= '2023-06-01'
  GROUP BY
    user_id
)
SELECT
  _s0.uid,
  COALESCE(_t0.agg_0, 0) AS total_duration
FROM main.users AS _s0
JOIN _t0 AS _t0
  ON _s0.uid = _t0.user_id
ORDER BY
  total_duration DESC
