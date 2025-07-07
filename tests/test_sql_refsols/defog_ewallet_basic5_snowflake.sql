WITH _u_0 AS (
  SELECT
    user_id AS _u_1
  FROM MAIN.NOTIFICATIONS
  GROUP BY
    user_id
)
SELECT
  USERS.uid,
  USERS.username
FROM MAIN.USERS AS USERS
LEFT JOIN _u_0 AS _u_0
  ON USERS.uid = _u_0._u_1
WHERE
  _u_0._u_1 IS NULL
