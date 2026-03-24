SELECT
  user_sessions.user_id AS uid,
  SUM(
    DATE_DIFF(
      'SECOND',
      CAST(DATE_TRUNC('SECOND', CAST(user_sessions.session_start_ts AS TIMESTAMP)) AS TIMESTAMP),
      CAST(DATE_TRUNC('SECOND', CAST(user_sessions.session_end_ts AS TIMESTAMP)) AS TIMESTAMP)
    )
  ) AS total_duration
FROM postgres.main.users AS users
JOIN postgres.main.user_sessions AS user_sessions
  ON user_sessions.session_end_ts < CAST('2023-06-08' AS DATE)
  AND user_sessions.session_start_ts >= CAST('2023-06-01' AS DATE)
  AND user_sessions.user_id = users.uid
GROUP BY
  1
ORDER BY
  2 DESC
