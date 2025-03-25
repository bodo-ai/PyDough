SELECT
  uid AS user_id
FROM (
  SELECT
    uid
  FROM main.users
) AS _table_alias_0
SEMI JOIN (
  SELECT
    user_id
  FROM (
    SELECT
      type AS notification_type,
      user_id
    FROM main.notifications
  ) AS _t0
  WHERE
    notification_type = 'transaction'
) AS _table_alias_1
  ON uid = user_id
