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
      ) AS _t3
      GROUP BY
        country
    ) AS _t2
  ) AS _t1
  ORDER BY
    ordering_1 DESC
  LIMIT 5
) AS _t0
ORDER BY
  ordering_1 DESC
