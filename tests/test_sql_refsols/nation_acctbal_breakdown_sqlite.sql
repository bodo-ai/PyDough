WITH "_t3" AS (
  SELECT
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY "customer"."c_nationkey" ORDER BY "customer"."c_acctbal" DESC) - 1.0
        ) - (
          CAST((
            COUNT("customer"."c_acctbal") OVER (PARTITION BY "customer"."c_nationkey") - 1.0
          ) AS REAL) / 2.0
        )
      ) < 1.0
      THEN "customer"."c_acctbal"
      ELSE NULL
    END AS "expr_7",
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY "customer"."c_nationkey" ORDER BY CASE WHEN "customer"."c_acctbal" < 0 THEN "customer"."c_acctbal" ELSE NULL END DESC) - 1.0
        ) - (
          CAST((
            COUNT(CASE WHEN "customer"."c_acctbal" < 0 THEN "customer"."c_acctbal" ELSE NULL END) OVER (PARTITION BY "customer"."c_nationkey") - 1.0
          ) AS REAL) / 2.0
        )
      ) < 1.0
      THEN CASE WHEN "customer"."c_acctbal" < 0 THEN "customer"."c_acctbal" ELSE NULL END
      ELSE NULL
    END AS "expr_8",
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY "customer"."c_nationkey" ORDER BY CASE WHEN "customer"."c_acctbal" >= 0 THEN "customer"."c_acctbal" ELSE NULL END DESC) - 1.0
        ) - (
          CAST((
            COUNT(CASE WHEN "customer"."c_acctbal" >= 0 THEN "customer"."c_acctbal" ELSE NULL END) OVER (PARTITION BY "customer"."c_nationkey") - 1.0
          ) AS REAL) / 2.0
        )
      ) < 1.0
      THEN CASE WHEN "customer"."c_acctbal" >= 0 THEN "customer"."c_acctbal" ELSE NULL END
      ELSE NULL
    END AS "expr_6",
    "customer"."c_nationkey" AS "nation_key",
    CASE WHEN "customer"."c_acctbal" < 0 THEN "customer"."c_acctbal" ELSE NULL END AS "negative_acctbal",
    CASE WHEN "customer"."c_acctbal" >= 0 THEN "customer"."c_acctbal" ELSE NULL END AS "non_negative_acctbal"
  FROM "tpch"."customer" AS "customer"
), "_t3_2" AS (
  SELECT
    AVG("_t3"."expr_6") AS "agg_0",
    AVG("_t3"."expr_7") AS "agg_1",
    AVG("_t3"."expr_8") AS "agg_2",
    COUNT("_t3"."negative_acctbal") AS "agg_4",
    COUNT("_t3"."non_negative_acctbal") AS "agg_3",
    "_t3"."nation_key" AS "nation_key"
  FROM "_t3" AS "_t3"
  GROUP BY
    "_t3"."nation_key"
)
SELECT
  "nation"."n_name" AS "nation_name",
  COALESCE("_t3"."agg_4", 0) AS "n_red_acctbal",
  COALESCE("_t3"."agg_3", 0) AS "n_black_acctbal",
  "_t3"."agg_2" AS "median_red_acctbal",
  "_t3"."agg_0" AS "median_black_acctbal",
  "_t3"."agg_1" AS "median_overall_acctbal"
FROM "tpch"."nation" AS "nation"
JOIN "tpch"."region" AS "region"
  ON "nation"."n_regionkey" = "region"."r_regionkey" AND "region"."r_name" = 'AMERICA'
LEFT JOIN "_t3_2" AS "_t3"
  ON "_t3"."nation_key" = "nation"."n_nationkey"
ORDER BY
  "nation"."n_name"
