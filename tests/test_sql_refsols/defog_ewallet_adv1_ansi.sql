SELECT
  name,
  (
    agg_0 * 1.0
  ) / agg_1 AS CPUR
FROM (
  SELECT
    agg_0,
    agg_1,
    name
  FROM (
    SELECT
      mid,
      name
    FROM main.merchants
  ) AS _table_alias_0
  INNER JOIN (
    SELECT
      COUNT(DISTINCT coupon_id) AS agg_0,
      COUNT(DISTINCT txid) AS agg_1,
      receiver_id
    FROM (
      SELECT
        coupon_id,
        receiver_id,
        txid
      FROM (
        SELECT
          coupon_id,
          receiver_id,
          status,
          txid
        FROM main.wallet_transactions_daily
      ) AS _t2
      WHERE
        status = 'success'
    ) AS _t1
    GROUP BY
      receiver_id
  ) AS _table_alias_1
    ON mid = receiver_id
) AS _t0
