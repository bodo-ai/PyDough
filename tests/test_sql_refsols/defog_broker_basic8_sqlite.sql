SELECT
  country,
  num_customers
FROM (
  SELECT
    COALESCE(agg_0, 0) AS num_customers,
    country
  FROM (
    SELECT
      COUNT() AS agg_0,
      country
    FROM (
      SELECT
        sbCustCountry AS country
      FROM main.sbCustomer
    )
    GROUP BY
      country
  )
)
ORDER BY
  num_customers DESC
LIMIT 5
