WITH _t AS (
  SELECT
    marketing_opt_in,
    user_id,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY CASE WHEN created_at IS NULL THEN 1 ELSE 0 END DESC, created_at DESC) AS _w
  FROM main.user_setting_snapshot
)
SELECT
  users.uid,
  _t.marketing_opt_in
FROM main.users AS users
JOIN _t AS _t
  ON _t._w = 1 AND _t.user_id = users.uid
