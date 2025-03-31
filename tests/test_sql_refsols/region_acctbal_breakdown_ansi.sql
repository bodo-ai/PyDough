SELECT
  region_name,
  n_red_acctbal,
  n_black_acctbal,
  median_red_acctbal,
  median_black_acctbal,
  median_overall_acctbal
FROM (
  SELECT
    COALESCE(agg_3, 0) AS n_black_acctbal,
    COALESCE(agg_4, 0) AS n_red_acctbal,
    agg_0 AS median_black_acctbal,
    agg_1 AS median_overall_acctbal,
    agg_2 AS median_red_acctbal,
    region_name AS ordering_5,
    region_name
  FROM (
    SELECT
      agg_0,
      agg_1,
      agg_2,
      agg_3,
      agg_4,
      region_name
    FROM (
      SELECT
        r_name AS region_name,
        r_regionkey AS key
      FROM tpch.REGION
    )
    LEFT JOIN (
      SELECT
        COUNT(negative_acctbal) AS agg_4,
        COUNT(non_negative_acctbal) AS agg_3,
        MEDIAN(acctbal) AS agg_1,
        MEDIAN(negative_acctbal) AS agg_2,
        MEDIAN(non_negative_acctbal) AS agg_0,
        region_key
      FROM (
        SELECT
          CASE WHEN acctbal >= 0 THEN acctbal ELSE NULL END AS non_negative_acctbal,
          CASE WHEN acctbal < 0 THEN acctbal ELSE NULL END AS negative_acctbal,
          acctbal,
          region_key
        FROM (
          SELECT
            acctbal,
            region_key
          FROM (
            SELECT
              n_nationkey AS key,
              n_regionkey AS region_key
            FROM tpch.NATION
          )
          INNER JOIN (
            SELECT
              c_acctbal AS acctbal,
              c_nationkey AS nation_key
            FROM tpch.CUSTOMER
          )
            ON key = nation_key
        )
      )
      GROUP BY
        region_key
    )
      ON key = region_key
  )
)
ORDER BY
  ordering_5
