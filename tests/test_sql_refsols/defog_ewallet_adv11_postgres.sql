WITH _s1 AS (
  SELECT
    COALESCE(
      SUM(
        CAST(EXTRACT(EPOCH FROM CAST(session_end_ts AS TIMESTAMP) - CAST(session_start_ts AS TIMESTAMP)) AS BIGINT)
      ),
      0
    ) AS total_duration,
    user_id
  FROM main.user_sessions
  WHERE
    session_end_ts < '2023-06-08' AND session_start_ts >= '2023-06-01'
  GROUP BY
    user_id
)
SELECT
  users.uid,
  _s1.total_duration
FROM main.users AS users
JOIN _s1 AS _s1
  ON _s1.user_id = users.uid
ORDER BY
  total_duration DESC NULLS LAST
