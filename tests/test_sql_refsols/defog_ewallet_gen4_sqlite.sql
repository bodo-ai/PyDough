SELECT
  merchants_id,
  merchant_registration_date,
  earliest_coupon_start_date,
  earliest_coupon_id
FROM (
  SELECT
    earliest_coupon_id,
    earliest_coupon_start_date,
    merchant_registration_date,
    merchants_id,
    start_date
  FROM (
    SELECT
      _table_alias_6.mid AS mid,
      agg_1 AS earliest_coupon_id,
      earliest_coupon_start_date,
      merchant_registration_date,
      merchants_id
    FROM (
      SELECT
        agg_0 AS earliest_coupon_start_date,
        created_at AS merchant_registration_date,
        mid AS merchants_id,
        mid
      FROM (
        SELECT
          created_at,
          mid
        FROM main.merchants
      ) AS _table_alias_0
      LEFT JOIN (
        SELECT
          MIN(start_date) AS agg_0,
          merchant_id
        FROM (
          SELECT
            merchant_id,
            start_date
          FROM main.coupons
        ) AS _t1
        GROUP BY
          merchant_id
      ) AS _table_alias_1
        ON mid = merchant_id
    ) AS _table_alias_6
    LEFT JOIN (
      SELECT
        MAX(cid) AS agg_1,
        mid
      FROM (
        SELECT
          cid,
          mid
        FROM (
          SELECT
            cid,
            earliest_coupon_start_date,
            mid,
            start_date
          FROM (
            SELECT
              agg_0 AS earliest_coupon_start_date,
              mid
            FROM (
              SELECT
                mid
              FROM main.merchants
            ) AS _table_alias_2
            LEFT JOIN (
              SELECT
                MIN(start_date) AS agg_0,
                merchant_id
              FROM (
                SELECT
                  merchant_id,
                  start_date
                FROM main.coupons
              ) AS _t4
              GROUP BY
                merchant_id
            ) AS _table_alias_3
              ON mid = merchant_id
          ) AS _table_alias_4
          INNER JOIN (
            SELECT
              cid,
              merchant_id,
              start_date
            FROM main.coupons
          ) AS _table_alias_5
            ON mid = merchant_id
        ) AS _t3
        WHERE
          earliest_coupon_start_date = start_date
      ) AS _t2
      GROUP BY
        mid
    ) AS _table_alias_7
      ON _table_alias_6.mid = _table_alias_7.mid
  ) AS _table_alias_8
  INNER JOIN (
    SELECT
      merchant_id,
      start_date
    FROM main.coupons
  ) AS _table_alias_9
    ON mid = merchant_id
) AS _t0
WHERE
  start_date <= DATETIME(merchant_registration_date, '1 year')
