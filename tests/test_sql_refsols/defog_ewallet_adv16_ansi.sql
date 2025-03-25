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
    ) AS _t1
    WHERE
      LOWER(country) = 'us'
  ) AS _table_alias_0
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
      ) AS _t3
      WHERE
        (
          notification_type = 'promotion'
        ) AND (
          status = 'unread'
        )
    ) AS _t2
    GROUP BY
      user_id
  ) AS _table_alias_1
    ON uid = user_id
) AS _t0
