WITH _t0 AS (
  SELECT
    SUM(
      DATEDIFF(CAST(session_end_ts AS DATETIME), CAST(session_start_ts AS DATETIME), SECOND)
    ) AS agg_0,
    user_id
  FROM main.user_sessions
  WHERE
    session_end_ts < '2023-06-08' AND session_start_ts >= '2023-06-01'
  GROUP BY
    user_id
)
SELECT
  users.uid,
  COALESCE(_t0.agg_0, 0) AS total_duration
FROM main.users AS users
JOIN _t0 AS _t0
  ON _t0.user_id = users.uid
ORDER BY
  total_duration DESC
