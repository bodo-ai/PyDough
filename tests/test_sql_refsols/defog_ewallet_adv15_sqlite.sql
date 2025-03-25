SELECT
  merchant_id_6 AS merchant_id,
  merchant_name,
  coupons_per_merchant
FROM (
  SELECT
    merchant_id_5 AS merchant_id_6,
    coupons_per_merchant,
    merchant_name,
    ordering_1
  FROM (
    SELECT
      COALESCE(agg_0, 0) AS coupons_per_merchant,
      COALESCE(agg_0, 0) AS ordering_1,
      mid AS merchant_id_5,
      name AS merchant_name
    FROM (
      SELECT
        agg_0,
        mid,
        name
      FROM (
        SELECT
          mid,
          name
        FROM main.merchants
      ) AS _table_alias_2
      LEFT JOIN (
        SELECT
          COUNT() AS agg_0,
          merchant_id
        FROM (
          SELECT
            merchant_id
          FROM (
            SELECT
              _table_alias_0.created_at AS created_at,
              _table_alias_1.created_at AS created_at_1,
              merchant_id
            FROM (
              SELECT
                created_at,
                merchant_id
              FROM main.coupons
            ) AS _table_alias_0
            LEFT JOIN (
              SELECT
                created_at,
                mid
              FROM main.merchants
            ) AS _table_alias_1
              ON merchant_id = mid
          ) AS _t4
          WHERE
            (
              (
                CAST(STRFTIME('%Y', created_at) AS INTEGER) - CAST(STRFTIME('%Y', created_at_1) AS INTEGER)
              ) * 12 + CAST(STRFTIME('%m', created_at) AS INTEGER) - CAST(STRFTIME('%m', created_at_1) AS INTEGER)
            ) = 0
        ) AS _t3
        GROUP BY
          merchant_id
      ) AS _table_alias_3
        ON mid = merchant_id
    ) AS _t2
  ) AS _t1
  ORDER BY
    ordering_1 DESC
  LIMIT 1
) AS _t0
ORDER BY
  ordering_1 DESC
