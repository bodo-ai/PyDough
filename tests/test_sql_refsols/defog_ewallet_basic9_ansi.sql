SELECT
  country,
  user_count,
  total_amount
FROM (
  SELECT
    country,
    ordering_2,
    total_amount,
    user_count
  FROM (
    SELECT
      COALESCE(agg_0, 0) AS ordering_2,
      COALESCE(agg_0, 0) AS total_amount,
      agg_1 AS user_count,
      country
    FROM (
      SELECT
        COUNT(DISTINCT sender_id) AS agg_1,
        SUM(amount) AS agg_0,
        country
      FROM (
        SELECT
          amount,
          country,
          sender_id
        FROM (
          SELECT
            amount,
            sender_id
          FROM (
            SELECT
              amount,
              sender_id,
              sender_type
            FROM main.wallet_transactions_daily
          ) AS _t4
          WHERE
            sender_type = 0
        ) AS _table_alias_0
        INNER JOIN (
          SELECT
            country,
            uid
          FROM main.users
        ) AS _table_alias_1
          ON sender_id = uid
      ) AS _t3
      GROUP BY
        country
    ) AS _t2
  ) AS _t1
  ORDER BY
    ordering_2 DESC
  LIMIT 5
) AS _t0
ORDER BY
  ordering_2 DESC
