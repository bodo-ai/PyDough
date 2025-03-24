SELECT
  username,
  COALESCE(agg_0, 0) AS total_unread_notifs
FROM (
  SELECT
    agg_0,
    username
  FROM (
    SELECT
      uid,
      username
    FROM (
      SELECT
        country,
        uid,
        username
      FROM main.users
    )
    WHERE
      LOWER(country) = 'us'
  )
  INNER JOIN (
    SELECT
      COUNT() AS agg_0,
      user_id
    FROM (
      SELECT
        user_id
      FROM (
        SELECT
          type AS notification_type,
          status,
          user_id
        FROM main.notifications
      )
      WHERE
        (
          notification_type = 'promotion'
        ) AND (
          status = 'unread'
        )
    )
    GROUP BY
      user_id
  )
    ON uid = user_id
)
