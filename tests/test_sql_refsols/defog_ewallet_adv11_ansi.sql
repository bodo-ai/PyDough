WITH _t1 AS (
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
  users.uid,
  COALESCE(_t1.agg_0, 0) AS total_duration
FROM main.users AS users
JOIN _t1 AS _t1
  ON _t1.user_id = users.uid
ORDER BY
  COALESCE(_t1.agg_0, 0) DESC
