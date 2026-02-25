WITH "_u_0" AS (
  SELECT
    NOTIFICATIONS.user_id AS "_u_1"
  FROM MAIN.NOTIFICATIONS NOTIFICATIONS
  JOIN MAIN.USERS USERS
    ON NOTIFICATIONS.created_at <= (
      CAST(USERS.created_at AS DATE) + NUMTOYMINTERVAL(1, 'year')
    )
    AND NOTIFICATIONS.created_at >= USERS.created_at
    AND NOTIFICATIONS.user_id = USERS.uid
  GROUP BY
    NOTIFICATIONS.user_id
)
SELECT
  USERS.username,
  USERS.email,
  USERS.created_at
FROM MAIN.USERS USERS
LEFT JOIN "_u_0" "_u_0"
  ON USERS.uid = "_u_0"."_u_1"
WHERE
  "_u_0"."_u_1" IS NULL
