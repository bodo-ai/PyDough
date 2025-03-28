WITH _table_alias_1 AS (
  SELECT
    SUM(
      (
        (
          CAST((
            JULIANDAY(DATE(user_sessions.session_end_ts, 'start of day')) - JULIANDAY(DATE(user_sessions.session_start_ts, 'start of day'))
          ) AS INTEGER) * 24 + CAST(STRFTIME('%H', user_sessions.session_end_ts) AS INTEGER) - CAST(STRFTIME('%H', user_sessions.session_start_ts) AS INTEGER)
        ) * 60 + CAST(STRFTIME('%M', user_sessions.session_end_ts) AS INTEGER) - CAST(STRFTIME('%M', user_sessions.session_start_ts) AS INTEGER)
      ) * 60 + CAST(STRFTIME('%S', user_sessions.session_end_ts) AS INTEGER) - CAST(STRFTIME('%S', user_sessions.session_start_ts) AS INTEGER)
    ) AS agg_0,
    user_sessions.user_id AS user_id
  FROM main.user_sessions AS user_sessions
  WHERE
    user_sessions.session_end_ts < '2023-06-08'
    AND user_sessions.session_start_ts >= '2023-06-01'
  GROUP BY
    user_sessions.user_id
)
SELECT
  users.uid AS uid,
  COALESCE(_table_alias_1.agg_0, 0) AS total_duration
FROM main.users AS users
JOIN _table_alias_1 AS _table_alias_1
  ON _table_alias_1.user_id = users.uid
ORDER BY
  COALESCE(_table_alias_1.agg_0, 0) DESC
