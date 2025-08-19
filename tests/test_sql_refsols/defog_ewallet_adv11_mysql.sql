WITH _s1 AS (
  SELECT
    SUM(TIMESTAMPDIFF(SECOND, session_start_ts, session_end_ts)) AS sum_duration,
    user_id
  FROM main.user_sessions
  WHERE
    session_end_ts < '2023-06-08' AND session_start_ts >= '2023-06-01'
  GROUP BY
    2
)
SELECT
  users.uid,
  COALESCE(_s1.sum_duration, 0) AS total_duration
FROM main.users AS users
JOIN _s1 AS _s1
  ON _s1.user_id = users.uid
ORDER BY
  2 DESC
