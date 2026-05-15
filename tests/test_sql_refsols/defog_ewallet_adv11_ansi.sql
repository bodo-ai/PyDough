SELECT
  user_sessions.user_id AS uid,
  SUM(
    DATEDIFF(
      CAST(user_sessions.session_end_ts AS DATETIME),
      CAST(user_sessions.session_start_ts AS DATETIME),
      SECOND
    )
  ) AS total_duration
FROM main.users AS users
JOIN main.user_sessions AS user_sessions
  ON user_sessions.session_end_ts < CAST('2023-06-08' AS DATE)
  AND user_sessions.session_start_ts >= CAST('2023-06-01' AS DATE)
  AND user_sessions.user_id = users.uid
GROUP BY
  1
ORDER BY
  2 DESC
