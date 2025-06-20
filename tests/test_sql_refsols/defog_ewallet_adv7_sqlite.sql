WITH _t AS (
  SELECT
    marketing_opt_in,
    user_id,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at DESC) AS _w
  FROM main.user_setting_snapshot
)
SELECT
  _s0.uid,
  _t.marketing_opt_in
FROM main.users AS _s0
JOIN _t AS _t
  ON _s0.uid = _t.user_id AND _t._w = 1
