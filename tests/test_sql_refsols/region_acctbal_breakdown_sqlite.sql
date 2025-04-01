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
        AVG(expr_6) AS agg_0,
        AVG(expr_7) AS agg_1,
        AVG(expr_8) AS agg_2,
        COUNT(negative_acctbal) AS agg_4,
        COUNT(non_negative_acctbal) AS agg_3,
        region_key
      FROM (
        SELECT
          CASE
            WHEN ABS(
              (
                ROW_NUMBER() OVER (PARTITION BY region_key ORDER BY acctbal DESC) - 1.0
              ) - (
                CAST((
                  COUNT(acctbal) OVER (PARTITION BY region_key) - 1.0
                ) AS REAL) / 2.0
              )
            ) < 1.0
            THEN acctbal
            ELSE NULL
          END AS expr_7,
          CASE
            WHEN ABS(
              (
                ROW_NUMBER() OVER (PARTITION BY region_key ORDER BY negative_acctbal DESC) - 1.0
              ) - (
                CAST((
                  COUNT(negative_acctbal) OVER (PARTITION BY region_key) - 1.0
                ) AS REAL) / 2.0
              )
            ) < 1.0
            THEN negative_acctbal
            ELSE NULL
          END AS expr_8,
          CASE
            WHEN ABS(
              (
                ROW_NUMBER() OVER (PARTITION BY region_key ORDER BY non_negative_acctbal DESC) - 1.0
              ) - (
                CAST((
                  COUNT(non_negative_acctbal) OVER (PARTITION BY region_key) - 1.0
                ) AS REAL) / 2.0
              )
            ) < 1.0
            THEN non_negative_acctbal
            ELSE NULL
          END AS expr_6,
          negative_acctbal,
          non_negative_acctbal,
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
      )
      GROUP BY
        region_key
    )
      ON key = region_key
  )
)
ORDER BY
  ordering_5
