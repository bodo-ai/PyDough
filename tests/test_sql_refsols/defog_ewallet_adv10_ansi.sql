SELECT
  uid AS user_id,
  COALESCE(agg_0, 0) AS total_transactions
FROM (
  SELECT
    agg_0,
    uid
  FROM (
    SELECT
      uid
    FROM main.users
  ) AS _table_alias_0
  INNER JOIN (
    SELECT
      COUNT() AS agg_0,
      sender_id
    FROM (
      SELECT
        sender_id
      FROM (
        SELECT
          sender_id,
          sender_type
        FROM main.wallet_transactions_daily
      ) AS _t2
      WHERE
        sender_type = 0
    ) AS _t1
    GROUP BY
      sender_id
  ) AS _table_alias_1
    ON uid = sender_id
) AS _t0
