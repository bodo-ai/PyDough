WITH _t1 AS (
  SELECT
    SUM(
      (
        (
          CAST((
            JULIANDAY(DATE(session_end_ts, 'start of day')) - JULIANDAY(DATE(session_start_ts, 'start of day'))
          ) AS INTEGER) * 24 + CAST(STRFTIME('%H', session_end_ts) AS INTEGER) - CAST(STRFTIME('%H', session_start_ts) AS INTEGER)
        ) * 60 + CAST(STRFTIME('%M', session_end_ts) AS INTEGER) - CAST(STRFTIME('%M', session_start_ts) AS INTEGER)
      ) * 60 + CAST(STRFTIME('%S', session_end_ts) AS INTEGER) - CAST(STRFTIME('%S', session_start_ts) AS INTEGER)
    ) AS agg_0,
    user_id AS user_id
  FROM main.user_sessions
  WHERE
    session_end_ts < '2023-06-08' AND session_start_ts >= '2023-06-01'
  GROUP BY
    user_id
)
SELECT
  users.uid AS uid,
  COALESCE(_t1.agg_0, 0) AS total_duration
FROM main.users AS users
JOIN _t1 AS _t1
  ON _t1.user_id = users.uid
ORDER BY
  COALESCE(_t1.agg_0, 0) DESC
