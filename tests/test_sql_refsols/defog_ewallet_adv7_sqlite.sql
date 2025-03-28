WITH _t AS (
  SELECT
    user_setting_snapshot.created_at AS created_at_1,
    user_setting_snapshot.marketing_opt_in AS marketing_opt_in,
    users.uid AS uid,
    ROW_NUMBER() OVER (PARTITION BY users.uid ORDER BY user_setting_snapshot.created_at DESC) AS _w
  FROM main.users AS users
  JOIN main.user_setting_snapshot AS user_setting_snapshot
    ON user_setting_snapshot.user_id = users.uid
)
SELECT
  _t.uid AS uid,
  _t.marketing_opt_in AS marketing_opt_in
FROM _t AS _t
WHERE
  _t._w = 1
