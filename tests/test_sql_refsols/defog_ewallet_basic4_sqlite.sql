SELECT
  uid AS user_id
FROM (
  SELECT
    uid
  FROM main.users
) AS _table_alias_0
WHERE
  EXISTS(
    SELECT
      1
    FROM (
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
    WHERE
      uid = user_id
  )
