SELECT
  week,
  COALESCE(agg_0, 0) AS num_notifs,
  COALESCE(agg_1, 0) AS weekend_notifs
FROM (
  SELECT
    COUNT() AS agg_0,
    SUM(is_weekend) AS agg_1,
    week
  FROM (
    SELECT
      DATE(
        created_at_1,
        '-' || (
          (
            CAST(STRFTIME('%w', DATETIME(created_at_1)) AS INTEGER) + 6
          ) % 7
        ) || ' days',
        'start of day'
      ) AS week,
      (
        (
          (
            CAST(STRFTIME('%w', created_at_1) AS INTEGER) + 6
          ) % 7
        )
      ) IN (5, 6) AS is_weekend
    FROM (
      SELECT
        created_at AS created_at_1
      FROM (
        SELECT
          uid
        FROM (
          SELECT
            country,
            uid
          FROM main.users
        )
        WHERE
          country IN ('US', 'CA')
      )
      INNER JOIN (
        SELECT
          created_at,
          user_id
        FROM main.notifications
        WHERE
          (
            created_at < DATE(
              'now',
              '-' || (
                (
                  CAST(STRFTIME('%w', DATETIME('now')) AS INTEGER) + 6
                ) % 7
              ) || ' days',
              'start of day'
            )
          )
          AND (
            created_at >= DATE(
              'now',
              '-' || (
                (
                  CAST(STRFTIME('%w', DATETIME('now')) AS INTEGER) + 6
                ) % 7
              ) || ' days',
              'start of day',
              '-21 day'
            )
          )
      )
        ON uid = user_id
    )
  )
  GROUP BY
    week
)
