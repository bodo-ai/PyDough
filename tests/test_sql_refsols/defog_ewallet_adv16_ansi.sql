WITH _table_alias_1 AS (
  SELECT
    COUNT() AS agg_0,
    notifications.user_id AS user_id
  FROM main.notifications AS notifications
  WHERE
    notifications.status = 'unread' AND notifications.type = 'promotion'
  GROUP BY
    notifications.user_id
)
SELECT
  users.username AS username,
  COALESCE(_table_alias_1.agg_0, 0) AS total_unread_notifs
FROM main.users AS users
JOIN _table_alias_1 AS _table_alias_1
  ON _table_alias_1.user_id = users.uid
WHERE
  LOWER(users.country) = 'us'
