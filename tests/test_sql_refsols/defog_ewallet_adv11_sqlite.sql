SELECT
  MAX(users.uid) AS uid,
  SUM(
    (
      (
        CAST((
          JULIANDAY(DATE(user_sessions.session_end_ts, 'start of day')) - JULIANDAY(DATE(user_sessions.session_start_ts, 'start of day'))
        ) AS INTEGER) * 24 + CAST(STRFTIME('%H', user_sessions.session_end_ts) AS INTEGER) - CAST(STRFTIME('%H', user_sessions.session_start_ts) AS INTEGER)
      ) * 60 + CAST(STRFTIME('%M', user_sessions.session_end_ts) AS INTEGER) - CAST(STRFTIME('%M', user_sessions.session_start_ts) AS INTEGER)
    ) * 60 + CAST(STRFTIME('%S', user_sessions.session_end_ts) AS INTEGER) - CAST(STRFTIME('%S', user_sessions.session_start_ts) AS INTEGER)
  ) AS total_duration
FROM main.users AS users
JOIN main.user_sessions AS user_sessions
  ON user_sessions.session_end_ts < '2023-06-08'
  AND user_sessions.session_start_ts >= '2023-06-01'
  AND user_sessions.user_id = users.uid
GROUP BY
  user_sessions.user_id
ORDER BY
  2 DESC
