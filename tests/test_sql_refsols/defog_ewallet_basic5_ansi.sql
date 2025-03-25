SELECT
  uid,
  username
FROM (
  SELECT
    uid,
    username
  FROM main.users
) AS _table_alias_0
ANTI JOIN (
  SELECT
    user_id
  FROM main.notifications
) AS _table_alias_1
  ON uid = user_id
