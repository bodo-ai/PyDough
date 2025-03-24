SELECT
  uid,
  marketing_opt_in
FROM (
  SELECT
    *
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY uid ORDER BY created_at_1 DESC) AS _w
    FROM (
      SELECT
        created_at AS created_at_1,
        marketing_opt_in,
        uid
      FROM (
        SELECT
          uid
        FROM main.users
      )
      INNER JOIN (
        SELECT
          created_at,
          marketing_opt_in,
          user_id
        FROM main.user_setting_snapshot
      )
        ON uid = user_id
    )
  ) AS _t
  WHERE
    _w = 1
)
