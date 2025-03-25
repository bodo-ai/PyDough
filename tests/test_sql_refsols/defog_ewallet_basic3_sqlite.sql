SELECT
  mid AS merchant
FROM (
  SELECT
    mid
  FROM main.merchants
) AS _table_alias_0
WHERE
  EXISTS(
    SELECT
      1
    FROM (
      SELECT
        receiver_id
      FROM (
        SELECT
          receiver_id,
          receiver_type
        FROM main.wallet_transactions_daily
      ) AS _t0
      WHERE
        receiver_type = 1
    ) AS _table_alias_1
    WHERE
      mid = receiver_id
  )
