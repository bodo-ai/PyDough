WITH _S1 AS (
  SELECT
    COUNT(*) AS N_ROWS,
    user_id AS USER_ID
  FROM MAIN.NOTIFICATIONS
  WHERE
    status = 'unread' AND type = 'promotion'
  GROUP BY
    user_id
)
SELECT
  USERS.username,
  _S1.N_ROWS AS total_unread_notifs
FROM MAIN.USERS AS USERS
JOIN _S1 AS _S1
  ON USERS.uid = _S1.USER_ID
WHERE
  LOWER(USERS.country) = 'us'
