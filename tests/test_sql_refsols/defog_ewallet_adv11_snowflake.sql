SELECT
  user_sessions.user_id AS uid,
  SUM(
    DATEDIFF(
      SECOND,
      CAST(user_sessions.session_start_ts AS DATETIME),
      CAST(user_sessions.session_end_ts AS DATETIME)
    )
  ) AS total_duration
FROM main.users AS users
JOIN main.user_sessions AS user_sessions
  ON user_sessions.session_end_ts < '2023-06-08'
  AND user_sessions.session_start_ts >= '2023-06-01'
  AND user_sessions.user_id = users.uid
GROUP BY
  1
ORDER BY
  2 DESC NULLS LAST
