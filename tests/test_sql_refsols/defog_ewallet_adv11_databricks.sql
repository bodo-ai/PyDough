SELECT
  user_sessions.user_id AS uid,
  SUM(
    (
      (
        DATEDIFF(
          DAY,
          CAST(user_sessions.session_start_ts AS DATE),
          CAST(user_sessions.session_end_ts AS DATE)
        ) * 24 + EXTRACT(HOUR FROM CAST(user_sessions.session_end_ts AS TIMESTAMP)) - EXTRACT(HOUR FROM CAST(user_sessions.session_start_ts AS TIMESTAMP))
      ) * 60 + EXTRACT(MINUTE FROM CAST(user_sessions.session_end_ts AS TIMESTAMP)) - EXTRACT(MINUTE FROM CAST(user_sessions.session_start_ts AS TIMESTAMP))
    ) * 60 + EXTRACT(SECOND FROM CAST(user_sessions.session_end_ts AS TIMESTAMP)) - EXTRACT(SECOND FROM CAST(user_sessions.session_start_ts AS TIMESTAMP))
  ) AS total_duration
FROM defog.ewallet.users AS users
JOIN defog.ewallet.user_sessions AS user_sessions
  ON user_sessions.session_end_ts < CAST('2023-06-08' AS DATE)
  AND user_sessions.session_start_ts >= CAST('2023-06-01' AS DATE)
  AND user_sessions.user_id = users.uid
GROUP BY
  1
ORDER BY
  2 DESC
