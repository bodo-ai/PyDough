WITH "_t1_2" AS (
  SELECT
    SUM(
      "lineitem"."l_extendedprice" * (
        1 - "lineitem"."l_discount"
      ) - "partsupp"."ps_supplycost" * "lineitem"."l_quantity"
    ) AS "agg_0",
    "nation"."n_name" AS "nation_name",
    CAST(STRFTIME('%Y', "orders"."o_orderdate") AS INTEGER) AS "o_year"
  FROM "tpch"."nation" AS "nation"
  JOIN "tpch"."supplier" AS "supplier"
    ON "nation"."n_nationkey" = "supplier"."s_nationkey"
  JOIN "tpch"."partsupp" AS "partsupp"
    ON "partsupp"."ps_suppkey" = "supplier"."s_suppkey"
  JOIN "tpch"."part" AS "part"
    ON "part"."p_name" LIKE '%green%' AND "part"."p_partkey" = "partsupp"."ps_partkey"
  JOIN "tpch"."lineitem" AS "lineitem"
    ON "lineitem"."l_partkey" = "partsupp"."ps_partkey"
    AND "lineitem"."l_suppkey" = "partsupp"."ps_suppkey"
  LEFT JOIN "tpch"."orders" AS "orders"
    ON "lineitem"."l_orderkey" = "orders"."o_orderkey"
  GROUP BY
    "nation"."n_name",
    CAST(STRFTIME('%Y', "orders"."o_orderdate") AS INTEGER)
)
SELECT
  "_t1"."nation_name" AS "NATION",
  "_t1"."o_year" AS "O_YEAR",
  COALESCE("_t1"."agg_0", 0) AS "AMOUNT"
FROM "_t1_2" AS "_t1"
ORDER BY
  "nation",
  "o_year" DESC
LIMIT 10
