SELECT
  uid,
  marketing_opt_in
FROM (
  SELECT
    uid
  FROM main.users
)
INNER JOIN (
  SELECT
    marketing_opt_in,
    user_id
  FROM (
    SELECT
      *
    FROM (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at DESC) AS _w
      FROM (
        SELECT
          created_at,
          marketing_opt_in,
          user_id
        FROM main.user_setting_snapshot
      )
    ) AS _t
    WHERE
      _w = 1
  )
)
  ON uid = user_id
