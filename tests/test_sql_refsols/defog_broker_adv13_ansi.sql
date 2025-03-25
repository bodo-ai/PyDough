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
    ) AS _t2
    WHERE
      join_date >= CAST('2023-01-01' AS DATE)
  ) AS _t1
  GROUP BY
    country
) AS _t0
