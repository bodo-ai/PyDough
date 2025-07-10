WITH _u_0 AS (
  SELECT
    user_id AS _u_1
  FROM MAIN.NOTIFICATIONS
  WHERE
    type = 'transaction'
  GROUP BY
    user_id
)
SELECT
  USERS.uid AS user_id
FROM MAIN.USERS AS USERS
LEFT JOIN _u_0 AS _u_0
  ON USERS.uid = _u_0._u_1
WHERE
  NOT _u_0._u_1 IS NULL
