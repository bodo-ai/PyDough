SELECT
  country,
  100 * COALESCE(COALESCE(agg_0, 0) / COALESCE(agg_1, 0), 0.0) AS ar
FROM (
  SELECT
    COUNT() AS agg_1,
    SUM(status = 'active') AS agg_0,
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
    ) AS _t2
    WHERE
      (
        join_date <= '2022-12-31'
      ) AND (
        join_date >= '2022-01-01'
      )
  ) AS _t1
  GROUP BY
    country
) AS _t0
