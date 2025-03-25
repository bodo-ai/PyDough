SELECT
  name AS merchant_name,
  COALESCE(agg_0, 0) AS total_coupons
FROM (
  SELECT
    agg_0,
    name
  FROM (
    SELECT
      mid,
      name
    FROM (
      SELECT
        category,
        mid,
        name,
        status
      FROM main.merchants
    ) AS _t1
    WHERE
      (
        status = 'active'
      ) AND (
        LOWER(category) LIKE '%%retail%%'
      )
  ) AS _table_alias_0
  INNER JOIN (
    SELECT
      COUNT() AS agg_0,
      merchant_id
    FROM (
      SELECT
        merchant_id
      FROM main.coupons
    ) AS _t2
    GROUP BY
      merchant_id
  ) AS _table_alias_1
    ON mid = merchant_id
) AS _t0
