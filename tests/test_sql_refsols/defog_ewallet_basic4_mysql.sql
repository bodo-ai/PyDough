WITH _u_0 AS (
  SELECT
    user_id AS _u_1
  FROM main.notifications
  WHERE
    type = 'transaction'
  GROUP BY
    1
)
SELECT
  users.uid AS user_id
FROM main.users AS users
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = users.uid
WHERE
  NOT _u_0._u_1 IS NULL
