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
    ) AS _table_alias_0
    INNER JOIN (
      SELECT
        SUM(duration) AS agg_0,
        user_id
      FROM (
        SELECT
          (
            (
              CAST((JULIANDAY(DATE(session_end_ts, 'start of day')) - JULIANDAY(DATE(session_start_ts, 'start of day'))) AS INTEGER) * 24 + CAST(STRFTIME('%H', session_end_ts) AS INTEGER) - CAST(STRFTIME('%H', session_start_ts) AS INTEGER)
            ) * 60 + CAST(STRFTIME('%M', session_end_ts) AS INTEGER) - CAST(STRFTIME('%M', session_start_ts) AS INTEGER)
          ) * 60 + CAST(STRFTIME('%S', session_end_ts) AS INTEGER) - CAST(STRFTIME('%S', session_start_ts) AS INTEGER) AS duration,
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
        ) AS _t3
      ) AS _t2
      GROUP BY
        user_id
    ) AS _table_alias_1
      ON uid = user_id
  ) AS _t1
) AS _t0
ORDER BY
  ordering_1 DESC
