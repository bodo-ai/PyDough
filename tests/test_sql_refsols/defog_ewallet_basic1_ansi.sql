SELECT
  month,
  COUNT(DISTINCT sender_id) AS active_users
FROM (
  SELECT
    DATE_TRUNC('MONTH', CAST(created_at AS TIMESTAMP)) AS month,
    sender_id
  FROM (
    SELECT
      created_at,
      sender_id
    FROM (
      SELECT
        created_at,
        sender_id
      FROM (
        SELECT
          created_at,
          sender_id,
          sender_type,
          status
        FROM main.wallet_transactions_daily
      ) AS _t2
      WHERE
        (
          EXTRACT(YEAR FROM created_at) = 2023
        )
        AND (
          sender_type = 0
        )
        AND (
          status = 'success'
        )
    ) AS _table_alias_0
    INNER JOIN (
      SELECT
        uid
      FROM (
        SELECT
          status,
          uid
        FROM main.users
      ) AS _t3
      WHERE
        status = 'active'
    ) AS _table_alias_1
      ON sender_id = uid
  ) AS _t1
) AS _t0
GROUP BY
  month
