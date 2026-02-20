SELECT
  user_sessions.user_id AS uid,
  SUM(
    TIMESTAMPDIFF(SECOND, user_sessions.session_start_ts, user_sessions.session_end_ts)
  ) AS total_duration
FROM ewallet.users AS users
JOIN ewallet.user_sessions AS user_sessions
  ON user_sessions.session_end_ts < '2023-06-08'
  AND user_sessions.session_start_ts >= '2023-06-01'
  AND user_sessions.user_id = users.uid
GROUP BY
  1
ORDER BY
  2 DESC
