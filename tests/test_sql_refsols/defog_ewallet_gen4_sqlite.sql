SELECT
  merchants_id,
  merchant_registration_date,
  earliest_coupon_start_date,
  earliest_coupon_id
FROM (
  SELECT
    _table_alias_0.mid AS mid,
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
    )
    LEFT JOIN (
      SELECT
        MIN(start_date) AS agg_0,
        merchant_id
      FROM (
        SELECT
          merchant_id,
          start_date
        FROM main.coupons
      )
      GROUP BY
        merchant_id
    )
      ON mid = merchant_id
  ) AS _table_alias_0
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
          agg_0 AS earliest_coupon_start_date,
          mid
        FROM (
          SELECT
            mid
          FROM main.merchants
        )
        LEFT JOIN (
          SELECT
            MIN(start_date) AS agg_0,
            merchant_id
          FROM (
            SELECT
              merchant_id,
              start_date
            FROM main.coupons
          )
          GROUP BY
            merchant_id
        )
          ON mid = merchant_id
      )
      INNER JOIN (
        SELECT
          cid,
          merchant_id,
          start_date
        FROM main.coupons
      )
        ON (
          mid = merchant_id
        ) AND (
          earliest_coupon_start_date = start_date
        )
    )
    GROUP BY
      mid
  ) AS _table_alias_1
    ON _table_alias_0.mid = _table_alias_1.mid
)
INNER JOIN (
  SELECT
    merchant_id,
    start_date
  FROM main.coupons
)
  ON (
    mid = merchant_id
  )
  AND (
    start_date <= DATETIME(merchant_registration_date, '1 year')
  )
