SELECT
  uid,
  total_duration
FROM (
  SELECT
    COALESCE(agg_0, 0) AS ordering_1,
    COALESCE(agg_0, 0) AS total_duration,
    uid
  FROM (
    SELECT
      agg_0,
      uid
    FROM (
      SELECT
        uid
      FROM main.users
    )
    INNER JOIN (
      SELECT
        SUM(duration) AS agg_0,
        user_id
      FROM (
        SELECT
          DATEDIFF(session_end_ts, session_start_ts, SECOND) AS duration,
          user_id
        FROM (
          SELECT
            session_end_ts,
            session_start_ts,
            user_id
          FROM main.user_sessions
          WHERE
            (
              session_end_ts < '2023-06-08'
            ) AND (
              session_start_ts >= '2023-06-01'
            )
        )
      )
      GROUP BY
        user_id
    )
      ON uid = user_id
  )
)
ORDER BY
  ordering_1 DESC
