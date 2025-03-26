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
    )
    WHERE
      (
        status = 'active'
      ) AND (
        LOWER(category) LIKE '%%retail%%'
      )
  )
  INNER JOIN (
    SELECT
      COUNT() AS agg_0,
      merchant_id
    FROM (
      SELECT
        merchant_id
      FROM main.coupons
    )
    GROUP BY
      merchant_id
  )
    ON mid = merchant_id
)
