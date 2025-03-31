SELECT
  country,
  100 * COALESCE(COALESCE(agg_0, 0) / COALESCE(agg_1, 0), 0.0) AS ar
FROM (
  SELECT
    COUNT() AS agg_1,
    SUM(expr_2) AS agg_0,
    country
  FROM (
    SELECT
      status = 'active' AS expr_2,
      country
    FROM (
      SELECT
        country,
        status
      FROM (
        SELECT
          sbCustCountry AS country,
          sbCustJoinDate AS join_date,
          sbCustStatus AS status
        FROM main.sbCustomer
      )
      WHERE
        (
          join_date <= '2022-12-31'
        ) AND (
          join_date >= '2022-01-01'
        )
    )
  )
  GROUP BY
    country
)
