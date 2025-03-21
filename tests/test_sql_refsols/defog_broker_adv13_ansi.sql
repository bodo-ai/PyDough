SELECT
  country AS cust_country,
  COALESCE(agg_0, 0) AS TAC
FROM (
  SELECT
    COUNT() AS agg_0,
    country
  FROM (
    SELECT
      country
    FROM (
      SELECT
        sbCustCountry AS country,
        sbCustJoinDate AS join_date
      FROM main.sbCustomer
    )
    WHERE
      join_date >= DATE_STR_TO_DATE('2023-01-01')
  )
  GROUP BY
    country
)
