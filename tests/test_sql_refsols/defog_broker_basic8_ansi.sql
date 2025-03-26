SELECT
  country,
  num_customers
FROM (
  SELECT
    country,
    num_customers,
    ordering_1
  FROM (
    SELECT
      COALESCE(agg_0, 0) AS num_customers,
      COALESCE(agg_0, 0) AS ordering_1,
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
    ordering_1 DESC
  LIMIT 5
)
ORDER BY
  ordering_1 DESC
