WITH _t AS (
  SELECT
    marketing_opt_in,
    user_id,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at DESC NULLS FIRST) AS _w
  FROM postgres.user_setting_snapshot
)
SELECT
  users.uid,
  _t.marketing_opt_in
FROM postgres.users AS users
JOIN _t AS _t
  ON _t._w = 1 AND _t.user_id = users.uid
